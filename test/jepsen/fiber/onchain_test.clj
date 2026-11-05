(ns jepsen.fiber.onchain-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.fiber [chain :as chain]
                          [onchain :as oc]
                          [rpc :as rpc]]))

(def locks {"n1" "L1", "n2" "L2", "n3" "L3"})

(def funding-hash (str "0x" (apply str (repeat 64 "a"))))

(defn outpoint-hex
  "A channel_outpoint as list_channels returns it."
  [hash index]
  (str hash (format "%02x000000" index)))

(defn record
  "A closed channel's record: amounts in shannons, as hex."
  [id local remote & {:keys [acceptor? offered received tlcs]}]
  {:channel_id           id
   :channel_outpoint     (outpoint-hex funding-hash 0)
   :is_acceptor          (boolean acceptor?)
   :state                {:state_name "Closed", :state_flags "COOPERATIVE"}
   :local_balance        (rpc/int->hex local)
   :remote_balance       (rpc/int->hex remote)
   :offered_tlc_balance  (rpc/int->hex (or offered 0))
   :received_tlc_balance (rpc/int->hex (or received 0))
   :pending_tlcs         (or tlcs [])})

(defn tx
  [hash block inputs outputs]
  {:hash    hash
   :status  "committed"
   :block   block
   :inputs  (vec (for [[lock cap] inputs] {:tx "0xprev", :index 0, :lock lock, :capacity cap}))
   :outputs (vec (map-indexed (fn [i [lock cap]] {:index i, :lock lock, :capacity cap}) outputs))})

(def funding-tx
  (tx funding-hash 5 [["L1" 2000]] [["F" 1000] ["L1" 999]]))

(defn channel-trace
  [txs & {:keys [live]}]
  {:outpoint {:tx funding-hash, :index 0}
   :capacity 1000
   :funding  funding-tx
   :txs      txs
   :live     (or live [])})

; n1 opened with 900 (800 + a reserve of 100), n2 accepted with 100 (its reserve).
; n1 then paid n2 100: n1 700, n2 100.
(def coop
  (channel-trace [(tx "0xshut" 20 [["F" 1000]] [["L1" 797] ["L2" 198]])]))

; n1 force-closes: the commitment fee (4) comes out of the funding cell; each
; settlement adds a wallet cell of 50 for its fee (3) and returns the change.
(def forced
  (channel-trace [(tx "0xcommit" 20 [["F" 1000]] [["C1" 996]])
                  (tx "0xsettle1" 31 [["C1" 996] ["L1" 50]] [["C2" 196] ["L1" 847]])
                  (tx "0xsettle2" 33 [["C2" 196] ["L2" 50]] [["L2" 243]])]))

(def ends
  {"n1" (record "0xc1" 700 100)
   "n2" (record "0xc1" 100 700 :acceptor? true)})

(deftest outpoints
  (is (= {:tx funding-hash, :index 0} (oc/parse-outpoint (outpoint-hex funding-hash 0))))
  (is (= {:tx funding-hash, :index 258} (oc/parse-outpoint (str funding-hash "02010000"))))
  (is (nil? (oc/parse-outpoint "0x12"))))

(deftest accounting
  (testing "nets count wallet inputs against outputs"
    (is (= {:fee 10, :net {"F" -1000, "C1" 0, "C2" 0, "L1" 797, "L2" 193}}
           (oc/accounting forced)))))

(deftest cooperative-close
  (let [c (oc/check-channel ends coop locks 0)]
    (is (= :ok (:status c)))
    (is (= 5 (:fee c)))
    (is (= {:local 700, :reserve 100, :expected 800, :received 797, :diff -3, :range [795 800], :ok? true}
           (get-in c [:parties "n1"])))
    (is (= [195 200] (get-in c [:parties "n2" :range])))))

(deftest force-close
  (let [c (oc/check-channel ends forced locks 0)]
    (is (= :ok (:status c)))
    (is (= 10 (:fee c)))
    (is (= 797 (get-in c [:parties "n1" :received])))
    (is (= 193 (get-in c [:parties "n2" :received])))))

(deftest wrong-amounts
  (testing "a party paid less than its balance and reserve, less every fee"
    (let [t (channel-trace [(tx "0xshut" 20 [["F" 1000]] [["L1" 850] ["L2" 140]])])
          c (oc/check-channel ends t locks 0)]
      (is (= :mismatch (:status c)))
      (is (false? (get-in c [:parties "n2" :ok?])))
      (is (= -60 (get-in c [:parties "n2" :diff])))
      (is (= 50 (get-in c [:parties "n1" :diff])))))
  (testing "a party paid more"
    (let [t (channel-trace [(tx "0xshut" 20 [["F" 1000]] [["L1" 900] ["L2" 99]])])]
      (is (= :mismatch (:status (oc/check-channel ends t locks 0))))))
  (testing "a third wallet paid"
    (let [t (channel-trace [(tx "0xshut" 20 [["F" 1000]] [["L1" 797] ["L2" 100] ["L3" 98]])])
          c (oc/check-channel ends t locks 0)]
      (is (= :misdirected (:status c)))
      (is (= {"n3" 98} (:stray c))))))

(deftest pending-tlcs
  (testing "a TLC n1 offered may go either way"
    (let [ends {"n1" (record "0xc1" 700 100 :offered 50)
                "n2" (record "0xc1" 100 700 :acceptor? true :received 50)}
          paid (channel-trace [(tx "0xshut" 20 [["F" 1000]] [["L1" 747] ["L2" 248]])])
          back (channel-trace [(tx "0xshut" 20 [["F" 1000]] [["L1" 797] ["L2" 198]])])]
      (is (= :ok (:status (oc/check-channel ends paid locks 0))))
      (is (= :ok (:status (oc/check-channel ends back locks 0))))
      (is (= [745 800] (get-in (oc/check-channel ends paid locks 0) [:parties "n1" :range])))
      (is (= [195 250] (get-in (oc/check-channel ends paid locks 0) [:parties "n2" :range]))))))

(deftest unsettled
  (let [live [{:tx "0xsettle1", :index 0, :lock "C2", :capacity 196}]
        t    (channel-trace (pop (:txs forced)) :live live)]
    (testing "a commitment cell left unspent"
      (is (= :unsettled (:status (oc/check-channel ends t locks 100000)))))
    (testing "unless one of its TLCs hasn't expired yet"
      (let [tlc  {:amount "0x32", :expiry (rpc/int->hex 90000)}
            ends (assoc-in ends ["n1" :pending_tlcs] [tlc])]
        (is (= :awaiting-expiry (:status (oc/check-channel ends t locks 100000))))
        (is (= :unsettled (:status (oc/check-channel ends t locks 200000)))))))
  (testing "a funding cell nobody spent: the channel never closed"
    (let [t (channel-trace [] :live [{:tx funding-hash, :index 0, :lock "F", :capacity 1000}])]
      (is (= :open (:status (oc/check-channel ends t locks 0))))))
  (testing "a funding transaction not on chain"
    (is (= :not-funded (:status (oc/check-channel ends {:funding-status "unknown"} locks 0))))))

(deftest analyze
  (let [settle   {:time     0
                  :waited-s 40
                  :locks    locks
                  :channels {"n1" [(get ends "n1")], "n2" [(get ends "n2")], "n3" []}
                  :traces   {"0xc1" forced}
                  :wallets  {"n1" {:capacity 10797, :block 40}
                             "n2" {:capacity 5193, :block 40}
                             "n3" {:capacity 7000, :block 40}}}
        baseline {:wallets {"n1" {:capacity 10000, :block 10}
                            "n2" {:capacity 5000, :block 10}
                            "n3" {:capacity 7000, :block 10}}}]
    (testing "every receipt and every wallet change explained"
      (let [r (oc/analyze baseline settle)]
        (is (true? (:valid? r)))
        (is (= {:ok 1} (:statuses r)))
        (is (= {:start 10000, :end 10797, :change 797, :explained 797, :txs 3, :diff 0}
               (get-in r [:wallets "n1"])))))
    (testing "a wallet change no transaction explains"
      (let [r (oc/analyze baseline (assoc-in settle [:wallets "n3" :capacity] 6000))]
        (is (false? (:valid? r)))
        (is (= ["n3"] (:unbalanced-wallets r)))))
    (testing "the funding transaction counts when it comes after the baseline"
      (let [r (oc/analyze (assoc-in baseline [:wallets "n1" :block] 1)
                          (assoc-in settle [:wallets "n1" :capacity] (+ 10797 -2000 999)))]
        (is (true? (:valid? r)))
        (is (= 4 (get-in r [:wallets "n1" :txs])))))
    (testing "an unsettled channel"
      (let [t (assoc forced :live [{:tx "0xcommit", :index 0, :lock "C1", :capacity 996}])
            r (oc/analyze baseline (assoc-in settle [:traces "0xc1"] t))]
        (is (false? (:valid? r)))
        (is (= :unsettled (get-in r [:problems "0xc1" :status])))))
    (testing "no baseline: the receipts are checked, the wallets aren't"
      (is (= :unknown (:valid? (oc/analyze nil settle)))))
    (testing "no settle read"
      (is (= :unknown (:valid? (oc/analyze baseline nil)))))))

;; Tracing against a fake chain

(defn script
  [args]
  {:code_hash "0xc0de", :hash_type "type", :args args})

(def fake-txs
  "hash -> [inputs as [hash index], outputs as [script capacity], block]. W1 and
  W2 are wallets; F the funding lock; C the commitment lock. 0xother spends
  another cell of F's lock, which the trace must not take for the channel's."
  {"0xfund"   [[["0xgen" 0]] [[(script "F") 1000] [(script "W1") 5000]] 5]
   "0xgen"    [[] [[(script "W1") 6001] [(script "F") 77]] 1]
   "0xother"  [[["0xgen" 1]] [[(script "W2") 77]] 8]
   "0xcommit" [[["0xfund" 0]] [[(script "C") 996]] 20]
   "0xsettle" [[["0xcommit" 0] ["0xfund" 1]] [[(script "W1") 5797] [(script "W2") 196]] 31]})

(defn fake-transaction
  [txs hash]
  (when-let [[ins outs block] (get txs hash)]
    {:transaction {:inputs  (vec (for [[h i] ins]
                                   {:previous_output {:tx_hash h, :index (rpc/int->hex i)}}))
                   :outputs (vec (for [[s cap] outs] {:capacity (rpc/int->hex cap), :lock s}))}
     :tx_status   {:status "committed", :block_number (rpc/int->hex block)}}))

(defn fake-transactions-of
  [txs s]
  (vec (for [[h [ins outs]] (sort-by (comp last val) txs)
             [io cells]     [["input" (for [[ph i] ins] (get-in txs [ph 1 i 0]))]
                             ["output" (map first outs)]]
             c              cells
             :when          (= s c)]
         {:tx_hash h, :io_type io})))

(defn trace-with
  [txs]
  (with-redefs [chain/transaction     (partial fake-transaction txs)
                chain/transactions-of (partial fake-transactions-of txs)]
    (oc/trace (atom {}) #{(chain/lock-key (script "W1")) (chain/lock-key (script "W2"))}
              {:tx "0xfund", :index 0})))

(deftest tracing
  (testing "from the funding cell through the commitment to the settlement"
    (let [t (trace-with fake-txs)]
      (is (= ["0xcommit" "0xsettle"] (map :hash (:txs t))))
      (is (= [] (:live t)))
      (is (false? (:truncated? t)))
      (is (= 1000 (:capacity t)))
      (is (= {:fee 7, :net {(chain/lock-key (script "F")) -1000
                            (chain/lock-key (script "C")) 0
                            (chain/lock-key (script "W1")) 797
                            (chain/lock-key (script "W2")) 196}}
             (oc/accounting t)))))
  (testing "a commitment cell nobody has spent yet"
    (let [t (trace-with (dissoc fake-txs "0xsettle"))]
      (is (= ["0xcommit"] (map :hash (:txs t))))
      (is (= [{:tx "0xcommit", :index 0, :capacity 996, :lock (chain/lock-key (script "C"))}]
             (:live t)))))
  (testing "a funding cell nobody has spent: the channel is open"
    (let [t (trace-with (dissoc fake-txs "0xcommit" "0xsettle"))]
      (is (= [] (:txs t)))
      (is (= [{:tx "0xfund", :index 0, :capacity 1000, :lock (chain/lock-key (script "F"))}]
             (:live t))))))

(def revocation-witness
  "A revocation witness of commitment 3, as Fiber's watchtower builds it."
  (str "0x10000000100000001000000010000000" "00" (format "%016x" 3)
       (apply str (repeat 64 "b")) (apply str (repeat 128 "c"))))

(deftest witnesses-and-lock-args
  (testing "a revocation witness gives the commitment number it revokes"
    (is (= 3 (oc/revoked-number revocation-witness))))
  (testing "a settlement (unlock count 1) or a wallet's witness isn't a revocation"
    (is (nil? (oc/revoked-number (str "0x10000000100000001000000010000000" "01"
                                      (subs revocation-witness 36)))))
    (is (nil? (oc/revoked-number (str "0x55000000100000005500000055000000" (apply str (repeat 138 "0"))))))
    (is (nil? (oc/revoked-number nil))))
  (testing "a commitment lock's args carry the commitment number at bytes 28-36"
    (is (= 7 (oc/commitment-number (str "0x" (apply str (repeat 56 "1")) (format "%016x" 7) "ffff")))))
  (testing "a wallet lock's 20-byte args don't"
    (is (nil? (oc/commitment-number (str "0x" (apply str (repeat 40 "1"))))))))

; n1 came back with an old state (800 / 0) and force-closed with it: its revoked
; commitment C1 (996) went on chain, and n2's watchtower revoked it, paying the
; whole cell to n2 and its fee (3) from a wallet cell of 50.
(def revoked
  (channel-trace [(tx "0xcommit" 20 [["F" 1000]] [["C1" 996]])
                  (assoc (tx "0xrevoke" 22 [["C1" 996] ["L2" 50]] [["L2" 996] ["L2" 47]])
                         :revokes 3)]))

(deftest punished
  (testing "the punisher got at least what it was owed: punished, not a mismatch"
    (let [c (oc/check-channel ends revoked locks 0)]
      (is (= :punished (:status c)))
      (is (= {:tx "0xrevoke", :revokes 3, :punisher "n2"} (:revocation c)))
      (is (= 993 (get-in c [:parties "n2" :received])))
      (is (= 0 (get-in c [:parties "n1" :received])))))
  (testing "a revocation that left the punisher short is a mismatch"
    (let [t (channel-trace [(tx "0xcommit" 20 [["F" 1000]] [["C1" 996]])
                            (assoc (tx "0xrevoke" 22 [["C1" 996] ["L2" 50]] [["L2" 100] ["L1" 943]])
                                   :revokes 3)])]
      (is (= :mismatch (:status (oc/check-channel ends t locks 0)))))))

(deftest stale-records
  (let [old      {"n1" (record "0xc1" 800 0)
                  "n2" (record "0xc1" 100 700 :acceptor? true)}
        settle   {:time 0, :waited-s 10, :locks locks
                  :channels {"n1" [(get old "n1")], "n2" [(get old "n2")], "n3" []}
                  :traces   {"0xc1" coop}}]
    (testing "a restored node's old record makes the latest payout look wrong"
      (is (= :mismatch (get-in (oc/analyze nil settle) [:problems "0xc1" :status]))))
    (testing "judged by its peer's record instead, the payout is right"
      (let [r (oc/analyze nil settle #{"n1"})]
        (is (= {:ok 1} (:statuses r)))
        (is (= ["n1"] (:stale-records r)))))
    (testing "the mirror swaps balances and TLC totals and keeps the stale end's state"
      (let [t (oc/trusted-ends (assoc-in old ["n1" :state] {:state_name "Stale"}) #{"n1"})]
        (is (= (get-in ends ["n1" :local_balance]) (get-in t ["n1" :local_balance])))
        (is (= (get-in ends ["n1" :remote_balance]) (get-in t ["n1" :remote_balance])))
        (is (= "Stale" (get-in t ["n1" :state :state_name])))))
    (testing "stale nodes come from the nemesis ops that list them"
      (is (= #{"n1"} (oc/stale-nodes [{:process :nemesis, :f :restore-state
                                       :value {:node "n1", :stale-store ["n1"]}}
                                      {:process 0, :f :pay, :value {:stale-store ["n2"]}}]))))))
