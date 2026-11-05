(ns jepsen.fiber.old-state-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.fiber [rpc :as rpc]
                          [schedule :as schedule]]
            [jepsen.fiber.faults [old-state :as os]]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :faults [:old-state], :old-state-targets [:one], :old-state-after 20
   :old-state-hold 40})

(defn ops
  [seed o]
  (os/ops (schedule/stream (schedule/streams seed) :old-state) o))

(deftest operations
  (let [xs (take 21 (ops 3 opts))]
    (testing "a sleep, then a save and a restore of the same node, over and over"
      (is (= {:type :sleep, :value 20} (first xs)))
      (is (= (take 20 (cycle [:save-state :restore-state])) (map :f (rest xs))))
      (doseq [[s r] (partition 2 (rest xs))]
        (is (= (:value s) (:value r)))
        (is (contains? (set nodes) (get-in s [:value :node]))))
      (is (< 1 (count (set (map (comp :node :value) (rest xs)))))))
    (testing "the same seed, the same targets"
      (is (= xs (take 21 (ops 3 opts))))))
  (testing "--old-state-count: that many pairs, then nothing"
    (is (= [:save-state :restore-state] (keep :f (take 21 (ops 3 (assoc opts :old-state-count 1)))))))
  (testing "middle: only the node that forwards"
    (is (= #{"n2"} (set (keep (comp :node :value) (take 21 (ops 4 (assoc opts :old-state-targets [:middle]))))))))
  (testing "not with the faults that move the store"
    (is (thrown? clojure.lang.ExceptionInfo (ops 1 (assoc opts :faults [:old-state :disk]))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (ops 1 (assoc opts :old-state-targets [:middle], :nodes ["n1" "n2"]))))))

(deftest pacing
  (is (= [0 40000000000 100000000000 40000000000 100000000000]
         (take 5 (os/gaps nil {:old-state-hold 40})))))

(def restore-value
  {:node         "n1"
   :mode         :raw
   :started      :started
   :moved        ["0xc1"]
   :fingerprints {:saved "aa", :before "bb", :restored "aa"}
   :after        {"0xc1" {:state "ChannelReady", :local 800, :remote 0, :commitment "0xold"}}
   :stale-store  ["n1"]})

(deftest effect
  (let [op #(hash-map :f :restore-state, :value (merge restore-value %))]
    (is (nil? (os/effect (op {}))))
    (is (string? (os/effect (op {:fingerprints {:saved "aa", :restored "cc"}}))))
    (testing "fiber mode rewrites the store (Stale flags), so only the channels tell"
      (is (nil? (os/effect (op {:mode :fiber, :fingerprints {:saved "aa", :restored "cc"}})))))
    (is (re-find #"moved on" (os/effect (op {:moved []}))))
    (is (re-find #"RPC" (os/effect (op {:started "RPC didn't come back"}))))
    (is (= "nothing saved" (os/effect {:f :restore-state, :value {:node "n1", :error "nothing saved"}})))
    (is (nil? (os/effect {:f :save-state, :value {}})))))

(deftest logs
  (let [f (os/log-findings
            ["  2026-11-05T17:16:47Z  WARN fnn::fiber::channel: peer: Pubkey(02ab) local_commitment_number (40, 41) peer_commitment_number (12 13) waiting_ack: false"
             "  2026-11-05T17:16:47Z  WARN fnn::fiber::channel: peer: Pubkey(02ab) local_commitment_number (40, 41) peer_commitment_number (41 40) waiting_ack: false"
             "  2026-11-05T17:16:47Z ERROR fnn::fiber::channel: Pubkey(02ab) Error while processing channel message: InvalidParameter(\"reestablish channel message with invalid commitment numbers\") with message: x"
             "  2026-11-05T17:16:48Z ERROR fnn::fiber::channel: Audit Failed for Stale channel: Local(L:12, R:13), Peer(L:41, R:40)"
             "  2026-11-05T17:16:50Z  WARN fnn::watchtower::actor: Found an old version commitment tx submitted by remote: 0xabc123"
             "  2026-11-05T17:16:50Z  INFO fnn::watchtower::actor: Revocation tx: TransactionView { } sent, tx_hash: Byte32(0xdef)"
             "  2026-11-05T17:16:51Z  WARN fnn::watchtower::actor: Found an old version commitment tx submitted by remote: 0xabc123"
             "  2026-11-05T17:16:51Z ERROR fnn::watchtower::actor: Failed to send revocation tx: TransactionView { }, error: Rpc(Error { code: ServerError(-1107), message: \"PoolRejectedDuplicatedTransaction: Transaction(Byte32(0xdef)) already exists in transaction_pool\" })"
             "  2026-11-05T17:16:52Z ERROR fnn::watchtower::actor: Failed to send revocation tx: TransactionView { }, error: Rpc(Error { message: \"Not enough capacity\" })"
             "  2026-11-05T17:16:53Z ERROR fnn::watchtower::actor: Cannot recover settlement snapshot for channel Hash256(0x03d7)"
             "  2026-11-05T17:16:54Z ERROR fnn::watchtower::actor: Cannot recover settlement snapshot for channel Hash256(0x03d7)"])]
    (is (= 1 (:reestablish-failed f)))
    (is (= [{:local [40 41], :peer [12 13]}] (:gaps f)))
    (is (= [{:local [12 13], :peer [41 40]}] (:stale-audits-failed f)))
    (testing "the watchtower finds the same commitment every check: listed once"
      (is (= ["0xabc123"] (:revoked-found f))))
    (is (= 1 (:revocations-sent f)))
    (testing "the pool turning down the same revocation again isn't a failure"
      (is (= 1 (:revocations-failed f))))
    (is (= ["0x03d7"] (:settlement-unrecoverable f)))))

(def locks {"n1" "L1", "n2" "L2", "n3" "L3"})

(defn record
  [local remote & {:keys [acceptor?]}]
  {:channel_id "0xc1", :state {:state_name "Closed"}, :is_acceptor (boolean acceptor?)
   :local_balance (rpc/int->hex local), :remote_balance (rpc/int->hex remote)
   :offered_tlc_balance "0x0", :received_tlc_balance "0x0", :pending_tlcs []})

(defn tx
  [hash inputs outputs & {:keys [revokes]}]
  (cond-> {:hash    hash
           :inputs  (vec (for [[lock cap] inputs] {:lock lock, :capacity cap}))
           :outputs (vec (map-indexed (fn [i [lock cap]] {:index i, :lock lock, :capacity cap}) outputs))}
    revokes (assoc :revokes revokes)))

(defn settle
  "n1 restored (its record says 800 / 0); n2's latest record: n2 100, n1 700. Each
  reserved 100, so n1 is owed 800 by the latest state and 900 by the old one."
  [txs]
  {:locks    locks
   :channels {"n1" [(record 800 0)], "n2" [(record 100 700 :acceptor? true)]}
   :traces   {"0xc1" {:capacity 1000, :txs txs, :live []}}})

(def old {:local 800, :remote 0})

(def commit
  (tx "0xcommit" [["F" 1000]] [[(str "C1") 996]]))

(deftest chain-outcomes
  (testing "punished: the peer's watchtower revoked the old commitment"
    (let [o (os/chain-outcome (settle [commit (tx "0xrev" [["C1" 996] ["L2" 50]] [["L2" 996] ["L2" 47]]
                                                  :revokes 3)])
                              "n1" "0xc1" old)]
      (is (= :punished (:status o)))
      (is (= "n2" (:punisher o)))
      (is (= 3 (:revoked-number o)))))
  (testing "a revocation paid to the restored node"
    (is (= :wrong-punisher
           (:status (os/chain-outcome (settle [commit (tx "0xrev" [["C1" 996] ["L1" 50]] [["L1" 1043]]
                                                          :revokes 3)])
                                      "n1" "0xc1" old)))))
  (testing "settled by the old balances: a revoked commitment went through"
    (let [o (os/chain-outcome (settle [commit (tx "0xs" [["C1" 996] ["L1" 50]] [["L1" 943] ["L2" 100]])])
                              "n1" "0xc1" old)]
      (is (= :revoked-unpunished (:status o)))
      (is (= {:latest 800, :old 900} (:restored-expected o)))))
  (testing "settled by the latest balances"
    (is (= :latest
           (:status (os/chain-outcome (settle [commit (tx "0xs" [["C1" 996] ["L1" 50]] [["L1" 843] ["L2" 200]])])
                                      "n1" "0xc1" old)))))
  (testing "a cooperative close"
    (is (= :cooperative
           (:status (os/chain-outcome (settle [(tx "0xshut" [["F" 1000]] [["L1" 198] ["L2" 797]])])
                                      "n1" "0xc1" old)))))
  (testing "old and latest balances alike: the payouts can't tell"
    (is (= :indistinguishable
           (:status (os/chain-outcome (settle [commit (tx "0xs" [["C1" 996] ["L1" 50]] [["L1" 843] ["L2" 200]])])
                                      "n1" "0xc1" {:local 700, :remote 100})))))
  (testing "a cell left unspent"
    (is (= :unsettled
           (:status (os/chain-outcome (assoc-in (settle [commit]) [:traces "0xc1" :live] [{:lock "C1"}])
                                      "n1" "0xc1" old))))))

(deftest verdict
  (let [restore {:process :nemesis, :type :info, :f :restore-state, :time 5, :value restore-value}
        punish  {:type :ok, :f :settle
                 :value (settle [commit (tx "0xrev" [["C1" 996] ["L2" 50]] [["L2" 996] ["L2" 47]]
                                            :revokes 3)])}
        cheat   {:type :ok, :f :settle
                 :value (settle [commit (tx "0xs" [["C1" 996] ["L1" 50]] [["L1" 943] ["L2" 100]])])}]
    (is (true? (:valid? (os/check [restore punish] {}))))
    (is (= {:punished 1} (:statuses (os/check [restore punish] {}))))
    (is (false? (:valid? (os/check [restore cheat] {}))))
    (testing "no settle read (no --onchain), or no restore: unknown"
      (is (= :unknown (:valid? (os/check [restore] {}))))
      (is (= :unknown (:valid? (os/check [punish] {})))))
    (testing "a share the restored node can't settle: locked, not a cheat"
      (let [locked {:type :ok, :f :settle
                    :value (assoc-in (settle [commit (tx "0xs" [["C1" 996]] [["L2" 100] ["C2" 895]])])
                                     [:traces "0xc1" :live] [{:index 1, :lock "C2", :capacity 895}])}]
        (is (= {:unsettled 1} (:statuses (os/check [restore locked] {}))))
        (let [r (os/check [restore locked] {"n1" {:settlement-unrecoverable ["0xc1"]}})]
          (is (= {:restored-share-locked 1} (:statuses r)))
          (is (= :unknown (:valid? r))))))))

(deftest excuses
  (let [history [{:process :nemesis, :type :info, :f :save-state, :time 100
                  :value {:node "n1", :fingerprint "aa"}}
                 {:process 0, :type :ok, :f :pay, :time 50, :value {:from "n1", :hash "0xbefore"}}
                 {:process 0, :type :ok, :f :pay, :time 150, :value {:from "n1", :hash "0xafter"}}
                 {:process 1, :type :ok, :f :pay, :time 150, :value {:from "n3", :hash "0xn3"}}
                 {:process :nemesis, :type :info, :f :restore-state, :time 200, :value restore-value}]
        r       {:valid?              false
                 :channel-problems    [{:channel "0xc1", :node "n2", :peer "n1", :problems [[:mirror {}]]}
                                       {:channel "0xc2", :node "n2", :peer "n3", :problems [[:pending-tlcs 1]]}]
                 :unfinished-payments [{:node "n1", :hash "0xold"}]
                 :success-flipped     [{:hash "0xbefore"} {:hash "0xafter"} {:hash "0xn3"}]
                 :probe-failures      [{:from "n2", :to "n1"} {:from "n2", :to "n3"}]}
        x       (os/excuse r history)]
    (is (= {"n1" 100} (os/restored history)))
    (testing "other ops' values aren't looked into (sorted maps with string keys throw)"
      (is (= {"n1" 100} (os/restored (cons {:process :nemesis, :type :info, :f :reconnect
                                            :value (sorted-map "n1->n2" :ok)}
                                           history)))))
    (testing "the restored node's channels, its own lost payments, probes to it"
      (is (= ["0xc2"] (map :channel (:channel-problems x))))
      (is (empty? (:unfinished-payments x)))
      (is (= ["0xafter"] (map :hash (get-in x [:old-state-excused :success-flipped]))))
      (is (= [{:from "n2", :to "n3"}] (:probe-failures x))))
    (testing "what it doesn't explain still fails the run"
      (is (= ["0xbefore" "0xn3"] (map :hash (:success-flipped x))))
      (is (false? (:valid? x))))
    (testing "nothing restored, or no final read: unchanged"
      (is (= r (os/excuse r (take 4 history))))
      (is (= {:valid? :unknown, :error "no final read"}
             (os/excuse {:valid? :unknown, :error "no final read"} history))))))
