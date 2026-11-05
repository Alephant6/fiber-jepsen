(ns jepsen.fiber.swap-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [cch :as cch]
                          [faults :as faults]
                          [schedule :as schedule]
                          [swap :as swap]]
            [jepsen.fiber.faults [cch-kill :as cch-kill]
                                 [lnd-disconnect :as lnd-disconnect]]))

(def opts
  {:nodes ["n1" "n2" "n3"], :swap-rate 2, :swap-amount [1000 20000]
   :swap-kinds [:send-btc :receive-btc]})

(defn swap-ops
  "The swaps the generator issues under Jepsen's simulator."
  [seed opts]
  (->> (gt/simulate (gen/context {:concurrency 2})
                    (gen/clients (gen/limit 100 (swap/generator opts (schedule/streams seed))))
                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 3000000000))))
       (filter #(= :invoke (:type %)))
       (map :value)))

(deftest ops
  (let [vs (map :value (take 500 (swap/ops (schedule/stream (schedule/streams 3) :swap) opts)))]
    (testing "both kinds, amounts in range"
      (is (= swap/kinds (set (map :kind vs))))
      (is (every? #(<= 1000 (:amount %) 20000) vs)))
    (testing "only receive-btc carries a preimage, of 32 bytes"
      (is (every? #(re-matches #"0x[0-9a-f]{64}" (:preimage %)) (filter #(= :receive-btc (:kind %)) vs)))
      (is (not-any? :preimage (filter #(= :send-btc (:kind %)) vs))))
    (testing "one kind when asked"
      (is (= #{:send-btc}
             (set (map (comp :kind :value)
                       (take 50 (swap/ops (schedule/stream (schedule/streams 3) :swap)
                                          (assoc opts :swap-kinds [:send-btc]))))))))
    (is (= [1000 20000] (swap/parse-range "1000-20000")))
    (is (= [5000 5000] (swap/parse-range "5000")))))

(deftest seeded
  (testing "the same seed gives the same swaps, whatever the timing"
    (let [a (swap-ops 11 opts)
          b (->> (gt/simulate (gen/context {:concurrency 2})
                              (gen/clients (gen/limit 100 (swap/generator opts (schedule/streams 11))))
                              (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 7000000000))))
                 (filter #(= :invoke (:type %)))
                 (map :value))]
      (is (< 50 (count a)))
      (is (= a b))
      (is (not= a (swap-ops 12 opts)))))
  (testing "swaps don't change the payments a seed gives"
    (let [pays #(take 50 (schedule/payments (schedule/stream (schedule/streams 5) :pay) 3))]
      (is (= (pays) (do (doall (take 50 (swap/ops (schedule/stream (schedule/streams 5) :swap) opts)))
                        (pays)))))))

(deftest encodings
  (is (= "0x00ff10" (cch/b64->hex (cch/hex->b64 "0x00ff10"))))
  (is (= "AP8Q" (cch/hex->b64 "00ff10")))
  (is (= 5 (cch/->long "5")))
  (is (= 5 (cch/->long 5)))
  (is (= {:result {:status "SUCCEEDED"}}
         (cch/lnd-stream-last "{\"result\":{\"status\":\"IN_FLIGHT\"}}\n{\"result\":{\"status\":\"SUCCEEDED\"}}\n"))))

(deftest configs
  (let [script {:code_hash "0xe1e3", :hash_type "data2", :args cch/wrapped-btc-args}
        cfg    (cch/cch-config {:nodes ["n1" "n2" "n3"]} script)
        y      (cch/yaml cfg)]
    (testing "CCH talks to the last node and to lnd-ingrid"
      (is (= "http://10.77.0.13:8227" (get-in cfg [:cch :fiber_rpc_url])))
      (is (= "https://10.77.0.21:10009" (get-in cfg [:cch :lnd_rpc_url])))
      (is (= ["rpc" "cch"] (:services cfg))))
    (testing "YAML, strings double-quoted with JSON escapes"
      (is (str/includes? y "rpc:\n  listening_addr: \"10.77.0.23:8227\"\n  enabled_modules:\n    - \"cch\"\n"))
      (is (str/includes? y "wrapped_btc_type_script: \"{\\\"code_hash\\\":\\\"0xe1e3\\\""))
      (is (str/includes? y "services:\n  - \"rpc\"\n  - \"cch\"\n")))
    (testing "LND keeps macaroons and TLS, which it needs to listen beyond loopback"
      (is (= "/fiber/cch/lnd-admin.macaroon" (get-in cfg [:cch :lnd_macaroon_path])))
      (is (not (str/includes? (cch/lnd-conf cch/hub-lnd) "no-macaroons")))
      (is (not (str/includes? (cch/lnd-conf cch/hub-lnd) "no-rest-tls"))))
    (is (str/includes? (cch/lnd-conf cch/hub-lnd) "tlsextraip=10.77.0.21"))
    (is (str/includes? (cch/lnd-conf cch/user-lnd) "bitcoind.zmqpubrawblock=tcp://10.77.0.20:28332"))))

;; Checker

(def user "n1")

(defn swap
  "A read swap: `kind`, order status, and the four legs' statuses."
  [id kind order [ckb-pay ckb-inv btc-pay btc-inv] & {:keys [amount fee] :or {amount 10100, fee 130}}]
  {:id id, :kind kind, :hash (str "0x" id)
   :order {:status order, :amount amount, :fee fee}
   :ckb-payment {:status ckb-pay, :fee 0}
   :ckb-invoice {:status ckb-inv}
   :btc-payment {:status btc-pay, :fee 0}
   :btc-invoice {:status btc-inv}})

(def completed-send ["Success" "Paid" "SUCCEEDED" "SETTLED"])
(def refunded-send ["Failed" "Cancelled" "FAILED" "OPEN"])

(defn udt
  [local remote]
  {user [{:channel_id "0xc1", :state {:state_name "ChannelReady"}
          :local_balance (str "0x" (Long/toHexString local))
          :remote_balance (str "0x" (Long/toHexString remote)) :pending_tlcs []}]
   "n3" [{:channel_id "0xc1", :state {:state_name "ChannelReady"}
          :local_balance (str "0x" (Long/toHexString remote))
          :remote_balance (str "0x" (Long/toHexString local)) :pending_tlcs []}]})

(defn ln
  [bob ingrid]
  {cch/user-lnd [{:chan-id "7", :active true, :local-balance bob, :remote-balance ingrid
                  :pending-htlcs 0}]
   cch/hub-lnd  [{:chan-id "7", :active true, :local-balance ingrid, :remote-balance bob
                  :pending-htlcs 0}]})

(defn read-op
  [swaps [ckb0 bob0] [ckb1 bob1]]
  {:type :ok, :f :swap-read
   :value {:swaps swaps
           :initial {:user user, :hub "n3", :udt-channels (udt ckb0 (- 2000000 ckb0))
                     :ln-channels (ln bob0 (- 1000000 bob0))}
           :final {:udt-channels (udt ckb1 (- 2000000 ckb1))
                   :ln-channels (ln bob1 (- 1000000 bob1))}}})

(defn check
  [history]
  (checker/check (swap/checker) {} history {}))

(deftest checker-valid
  (testing "a completed send and a refunded one; the balances moved by the completed one"
    (let [r (check [{:type :ok, :f :swap, :value {:hash "0x1"}}
                    {:type :fail, :f :swap, :value {:hash "0x2"}}
                    (read-op [(swap 1 :send-btc "Success" completed-send)
                              (swap 2 :send-btc "Failed" refunded-send)]
                             [1000000 500000] [(- 1000000 10100) (+ 500000 9970)])])]
      (is (true? (:valid? r)) (pr-str r))
      (is (= {:completed 1, :refunded 1} (:outcomes r)))))
  (testing "a completed receive: the BTC user pays the amount, the CKB user gets it less the fee"
    (let [r (check [(read-op [(swap 1 :receive-btc "Success" completed-send)]
                             [1000000 500000] [(+ 1000000 9970) (- 500000 10100)])])]
      (is (true? (:valid? r)) (pr-str r))))
  (testing "an order nobody paid is fine"
    (let [r (check [(read-op [(swap 1 :send-btc "Success" completed-send)
                              (swap 2 :receive-btc "Pending" [:none "Open" :none "OPEN"])]
                             [1000000 500000] [(- 1000000 10100) (+ 500000 9970)])])]
      (is (true? (:valid? r)) (pr-str r))
      (is (= {:completed 1, :unpaid 1} (:outcomes r))))))

(deftest checker-invalid
  (testing "one side paid: the hub paid the BTC user but never collected"
    (let [r (check [(read-op [(swap 1 :send-btc "Success" completed-send)
                              (swap 2 :send-btc "Failed" ["Failed" "Cancelled" "SUCCEEDED" "SETTLED"])]
                             [1000000 500000] [(- 1000000 10100) (+ 500000 9970 9970)])])]
      (is (false? (:valid? r)))
      (is (= [[:one-sided {:loser :hub, :btc-paid true, :ckb-paid false}]
              [:paid-but-order "Failed"]]
             (:problems (first (:broken r)))))
      (testing "the balances agree with the records, which show the loss"
        (is (true? (get-in r [:balances :valid?]))))))
  (testing "money still held"
    (let [r (check [(read-op [(swap 1 :send-btc "Success" completed-send)
                              (swap 2 :send-btc "IncomingAccepted" ["Inflight" "Received" :none "OPEN"])]
                             [1000000 500000] [(- 1000000 10100) (+ 500000 9970)])])]
      (is (false? (:valid? r)))
      (is (= [[:order-unfinished "IncomingAccepted"]
              [:held {:ckb-payment "Inflight", :ckb-invoice "Received"}]]
             (:problems (first (:broken r)))))))
  (testing "the client saw the payer's payment succeed, but the swap ended unpaid"
    (let [r (check [{:type :ok, :f :swap, :value {:hash "0x2"}}
                    (read-op [(swap 1 :send-btc "Success" completed-send)
                              (swap 2 :send-btc "Failed" refunded-send)]
                             [1000000 500000] [(- 1000000 10100) (+ 500000 9970)])])]
      (is (false? (:valid? r)))
      (is (= [[:client-saw-success]] (:problems (first (:broken r)))))))
  (testing "a balance that doesn't match the paid swaps"
    (let [r (check [(read-op [(swap 1 :send-btc "Success" completed-send)]
                             [1000000 500000] [(- 1000000 10100) (+ 500000 9000)])])]
      (is (false? (:valid? r)))
      (is (= {:delta 9000, :expected 9970} (get-in r [:balances :btc-user]))))))

(deftest checker-unknown
  (is (= :unknown (:valid? (check []))))
  (testing "an unreadable order"
    (let [s (assoc (swap 1 :send-btc "Success" completed-send) :order {:status :unknown})]
      (is (= :unknown (:valid? (check [(read-op [s] [1000000 500000] [1000000 500000])]))))))
  (testing "no swap completed"
    (let [r (check [(read-op [(swap 1 :send-btc "Failed" refunded-send)]
                             [1000000 500000] [1000000 500000])])]
      (is (= :unknown (:valid? r)))
      (is (= "no swap completed" (:error r))))))

;; Faults and options

(defn fault-ops
  [seed fs]
  (->> (gt/simulate (gen/context {:concurrency 1})
                    (gen/nemesis (gen/limit 200 (faults/generator (schedule/streams seed)
                                                                  {:nodes ["n1" "n2" "n3"]
                                                                   :nemesis-interval 2
                                                                   :kill-targets [:one :all]
                                                                   :lnd-disconnect-targets [:cch :peer]
                                                                   :faults fs})))
                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 1000000))))
       (filter #(= :info (:type %)))
       (map (juxt :f :value))))

(deftest swap-faults
  (testing "each kill of CCH is followed by its restart"
    (is (= (take 6 (cycle [:kill-cch :start-cch]))
           (map :f (take 6 (cch-kill/ops nil {}))))))
  (testing "lnd-disconnect draws its links and heals each cut"
    (let [ops (take 200 (lnd-disconnect/ops (schedule/stream (schedule/streams 4) :lnd-disconnect)
                                            {:lnd-disconnect-targets [:cch :peer]
                                             :lnd-disconnect-mode :reject}))]
      (is (= (take 200 (cycle [:start-lnd-disconnect :stop-lnd-disconnect])) (map :f ops)))
      (is (= #{{:link :cch, :mode :reject} {:link :peer, :mode :reject}}
             (set (keep :value ops))))))
  (testing "rules cut both directions"
    (is (= [["OUTPUT" "-d" "10.77.0.21" "-j" "DROP"] ["INPUT" "-s" "10.77.0.21" "-j" "DROP"]]
           (lnd-disconnect/rules "10.77.0.21" :drop))))
  (testing "a cut took effect unless it failed or left the LNDs connected"
    (let [cut (fn [v] {:type :info, :f :start-lnd-disconnect, :value v})
          eff lnd-disconnect/effect]
      (is (nil? (eff (cut {:link :cch, :mode :drop, :result :cut}))))
      (is (nil? (eff (cut {:link :peer, :mode :drop, :result :cut
                           :connected {"lnd-bob" false, "lnd-ingrid" false}}))))
      (is (= "failed: ssh" (eff (cut {:link :cch, :mode :drop, :result "failed: ssh"}))))
      (is (= "still connected after the cut: lnd-bob"
             (eff (cut {:link :peer, :mode :drop, :result :cut
                        :connected {"lnd-bob" true, "lnd-ingrid" false}}))))
      (is (= "disconnecting the LNDs failed: 500"
             (eff (cut {:link :peer, :mode :drop, :result :cut, :connected "failed: 500"}))))
      (is (nil? (eff {:type :info, :f :stop-lnd-disconnect, :value {:reconnect "failed: x"}})))))
  (testing "adding the swap faults changes no kill"
    (doseq [seed [1 2]]
      (let [kill  #(filter (comp #{:kill :start} first) %)
            alone (kill (fault-ops seed [:kill]))
            mixed (kill (fault-ops seed [:kill :cch-kill :lnd-disconnect]))
            n     (min (count alone) (count mixed))]
        (is (< 10 n))
        (is (= (take n alone) (take n mixed)))))))

(deftest options
  (let [parse #(cli/parse-opts % fiber/cli-opts)]
    (let [{:keys [options errors]} (parse ["--cch" "--faults" "cch-kill,lnd-disconnect"
                                           "--swap-amount" "500-900" "--swap-kinds" "receive-btc"
                                           "--lnd-disconnect-targets" "peer"
                                           "--lnd-disconnect-mode" "reject"])]
      (is (empty? errors))
      (is (:cch options))
      (is (= [500 900] (:swap-amount options)))
      (is (= [:receive-btc] (:swap-kinds options)))
      (is (= [:peer] (:lnd-disconnect-targets options)))
      (is (= :reject (:lnd-disconnect-mode options)))
      (is (= 0.5 (:swap-rate options))))
    (is (seq (:errors (parse ["--swap-kinds" "swap-ckb"]))))
    (is (seq (:errors (parse ["--lnd-disconnect-targets" "chain"])))))
  (testing "the swap faults need --cch"
    (is (thrown-with-msg? IllegalArgumentException #"needs --cch"
                          (swap/check-opts! {:faults [:kill :cch-kill]})))
    (is (nil? (swap/check-opts! {:faults [:cch-kill], :cch true})))
    (is (nil? (swap/check-opts! {:faults [:kill]})))))
