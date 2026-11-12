(ns jepsen.fiber.closes-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.fiber [closes :as closes]
                          [rpc :as rpc]
                          [topology :as topology]]))

(defn channel
  "`state` is a state name, or fnn's whole state: {:state_name, :state_flags}."
  ([id local remote] (channel id local remote "ChannelReady" []))
  ([id local remote state tlcs]
   {:channel_id     id
    :state          (if (map? state) state {:state_name state})
    :local_balance  (rpc/int->hex local)
    :remote_balance (rpc/int->hex remote)
    :pending_tlcs   tlcs}))

(def nodes ["n1" "n2" "n3" "n4"])

(def ids
  "A ring n1 - n2 - n3 - n4 - n1."
  {"0x12" ["n1" "n2"], "0x23" ["n2" "n3"], "0x34" ["n3" "n4"], "0x41" ["n4" "n1"]})

(defn ring
  "Every channel 1000 CKB on both ends; `states` maps [channel node] to a state, and
  `tlcs` maps [channel node] to pending TLCs."
  ([] (ring {} {}))
  ([states tlcs]
   (reduce (fn [m [id [a b]]]
             (let [ch (fn [n] (channel id 100000000000 100000000000 (get states [id n] "ChannelReady")
                                       (get tlcs [id n] [])))]
               (-> m (update a (fnil conj []) (ch a)) (update b (fnil conj []) (ch b)))))
           {}
           (sort ids))))

(defn run
  "The --force-close-ok checker over a history that ends with `final`, with a
  :pay that the client saw finish and `probe` as the probe."
  [final pays probe]
  (let [test {:nodes nodes, :topology :ring, :initial-channels (atom (ring))}]
    (checker/check (closes/checker) test
                   (concat pays
                           [{:type :ok, :f :probe, :value probe}
                            {:type  :ok
                             :f     :read
                             :value {:channels final
                                     :payments (for [p pays]
                                                 {:node   (get-in p [:value :from])
                                                  :hash   (get-in p [:value :hash])
                                                  :status (:final p)})}}])
                   {})))

(defn probe
  [from to status & [route?]]
  (cond-> {:from from, :to to, :amount 1000000, :status status}
    (not= "Success" status) (assoc :error "Failed to build route")
    route?                  (assoc :route? true)))

(def all-probes
  (vec (for [[from to route?] (topology/probe-pairs {:nodes nodes, :topology :ring
                                                     :probe-routes true})]
         (probe from to "Success" route?))))

(deftest nothing-closed
  (let [r (run (ring) [] all-probes)]
    (is (= true (:valid? r)))
    (is (= {:channels []} (:force-closed r)))
    (is (= 12 (count all-probes)))
    (is (= 4 (count (filter :route? all-probes))))))

(deftest a-closed-channel-and-what-it-explains
  (let [closed {["0x23" "n2"] "Closed", ["0x23" "n3"] "Closed"}
        tlc    [{:payment_hash "0xh1", :amount "0x10"}]
        final  (ring closed {["0x23" "n2"] tlc})
        pays   [{:type :info, :f :pay, :value {:from "n1", :to "n3", :hash "0xh1"}, :final "Inflight"}]
        probes (mapv #(if (#{["n2" "n3"] ["n3" "n2"]} [(:from %) (:to %)])
                        (assoc % :status "Failed", :error "Insufficient balance")
                        %)
                     all-probes)
        r      (run final pays probes)]
    (testing "the closed channel, its stuck payment and its probes are excused"
      (is (= true (:valid? r)))
      (is (= ["0x23"] (get-in r [:force-closed :channels])))
      (is (= 2 (count (get-in r [:force-closed :excused :channel-problems]))))
      (is (= ["0xh1"] (map :hash (get-in r [:force-closed :excused :unfinished-payments]))))
      (is (= 2 (count (get-in r [:force-closed :excused :probe-failures])))))
    (testing "and so is the upstream TLC of that payment on a channel still open"
      (let [final (ring closed {["0x23" "n2"] tlc, ["0x12" "n1"] tlc, ["0x12" "n2"] tlc})
            r     (run final pays probes)]
        (is (= true (:valid? r)))
        (is (= 4 (count (get-in r [:force-closed :excused :channel-problems]))))))
    (testing "but not a TLC for another payment"
      (let [other [{:payment_hash "0xh9", :amount "0x10"}]
            r     (run (ring closed {["0x23" "n2"] tlc, ["0x12" "n1"] other}) pays probes)]
        (is (= false (:valid? r)))
        (is (= [[[:pending-tlcs 1]]] (map :problems (:channel-problems r))))))
    (testing "the fiber checker alone fails it"
      (let [test {:nodes nodes, :topology :ring, :initial-channels (atom (ring))}]
        (is (= false (:valid? (checker/check (jepsen.fiber.checker/checker) test
                                             (concat pays [{:type :ok, :f :probe, :value probes}
                                                           {:type :ok, :f :read
                                                            :value {:channels final
                                                                    :payments [{:node "n1"
                                                                                :hash "0xh1"
                                                                                :status "Inflight"}]}}])
                                             {}))))))))

(deftest what-closed-channels-do-not-explain
  (let [closed {["0x23" "n2"] "Closed", ["0x23" "n3"] "Closed"}]
    (testing "a stuck payment no closed channel holds"
      (let [r (run (ring closed {})
                   [{:type :info, :f :pay, :value {:from "n1", :to "n3", :hash "0xh2"}, :final "Inflight"}]
                   all-probes)]
        (is (= false (:valid? r)))
        (is (= ["0xh2"] (map :hash (:unfinished-payments r))))))
    (testing "a channel only one end considers closed"
      (let [r (run (ring {["0x23" "n2"] "Closed"} {}) [] all-probes)]
        (is (= false (:valid? r)))
        (is (= [] (get-in r [:force-closed :channels])))
        (is (= ["0x23"] (get-in r [:force-closed :not-settled])))
        (is (= "1" (:closes-not-settled r)))))
    (testing "a route probe between nodes still joined by ready channels"
      ; n1 -> n3 can still go through n4.
      (let [r (run (ring closed {}) []
                   (mapv #(if (and (= "n1" (:from %)) (= "n3" (:to %))) (assoc % :status "Failed") %)
                         all-probes))]
        (is (= false (:valid? r)))
        (is (= [["n1" "n3"]] (map (juxt :from :to) (:probe-failures r))))))
    (testing "a route probe between nodes the closed channels cut apart"
      ; With 0x23 and 0x41 closed, only n1 - n2 and n3 - n4 remain.
      (let [both {["0x23" "n2"] "Closed", ["0x23" "n3"] "Closed"
                  ["0x41" "n4"] "Closed", ["0x41" "n1"] "Closed"}
            r    (run (ring both {}) []
                      (mapv #(if (or (:route? %)
                                     (#{#{"n2" "n3"} #{"n4" "n1"}} (set [(:from %) (:to %)])))
                               (assoc % :status "Failed")
                               %)
                            all-probes))]
        (is (= true (:valid? r)))
        (is (= 8 (count (get-in r [:force-closed :excused :probe-failures]))))))))

(deftest probe-pairs
  (is (= [["n1" "n2" false] ["n2" "n1" false] ["n2" "n3" false] ["n3" "n2" false]]
         (topology/probe-pairs {:nodes ["n1" "n2" "n3"]})))
  (is (= [["n1" "n3" true] ["n3" "n1" true]]
         (filter last (topology/probe-pairs {:nodes ["n1" "n2" "n3"], :probe-routes true}))))
  (testing "six nodes in a mesh: six pairs, each way"
    (is (= 12 (count (filter last (topology/probe-pairs {:nodes ["n1" "n2" "n3" "n4" "n5" "n6"]
                                                         :topology :mesh, :probe-routes true})))))))

(defn nem
  "A nemesis completion at `s` seconds, after its invocation."
  [s f value]
  [{:type :info, :process :nemesis, :f f, :time (long (* 1e9 (- s 0.5)))}
   {:type :info, :process :nemesis, :f f, :value value, :time (long (* 1e9 s))}])

(def epoch-ms
  "The wall-clock time of the history's time 0 in the tests with logs:
  2026-01-01T00:00:00Z."
  1767225600000)

(defn wall
  "`ops` with the wall clock on each completion, as jepsen.fiber.health/stamped
  leaves them."
  [ops]
  (for [op ops]
    (cond-> op
      (contains? op :value) (assoc :wall-ms (+ epoch-ms (quot (:time op) 1000000))))))

(defn at
  "fnn's log timestamp `s` seconds into the history."
  [s]
  (str (java.time.Instant/ofEpochMilli (+ epoch-ms (long (* 1000 s))))))

(defn inactive
  [s id]
  (str (at s) " ERROR fnn::fiber::channel: Channel Hash256(" id ") from peer Pubkey(02ab) is inactive"
       " for a time, shutting down it forcefully"))

(defn broadcast
  [s id]
  (str (at s) "  INFO fnn::fiber::network: Channel (Hash256(" id ")) to peer Pubkey(02ab) is closed"
       " Byte32(0x77). Broadcasting closing transaction (\"forcefully\") now."))

(def faults-history
  (vec (concat (nem 10 :kill {"n2" :killed})
               (nem 12 :pause {"n4" "T"})
               (nem 20 :pause {"n1" "S"})          ; not stopped: no spell
               (nem 25 :resume {"n1" "S", "n2" :not-running, "n3" "S", "n4" "S"})
               (nem 30 :start-partition [:isolated {"n3" #{"n4"}, "n4" #{"n3"}}])
               (nem 50 :start {"n1" :already-running, "n2" :started, "n3" :already-running
                               "n4" :already-running})
               (nem 60 :kill {"n1" :killed})
               (nem 100 :stop-partition :network-healed)
               (nem 105 :health {})
               (nem 120 :start {"n1" :started}))))

(deftest outages
  (is (= [{:kind :pause, :node "n4", :seconds 13.0}
          {:kind :kill, :node "n2", :seconds 40.0}
          {:kind :kill, :node "n1", :seconds 60.0}
          {:kind :partition, :nodes #{"n3" "n4"}, :seconds 70.0}]
         (sort-by :seconds (map #(dissoc % :began :from :to) (closes/outages faults-history)))))
  (testing "a spell begins when its op was invoked, and counts from when it completed"
    (is (= [[(long 9.5e9) (long 10e9) (long 50e9)]]
           (for [s (closes/outages faults-history) :when (= "n2" (:node s))]
             ((juxt :began :from :to) s)))))
  (testing "a spell nothing ends lasts until the history does"
    (is (= [{:kind :kill, :node "n2", :seconds 30.0}]
           (map #(dissoc % :began :from :to)
                (closes/outages (concat (nem 10 :kill {"n2" :killed})
                                        [{:type :ok, :process 0, :f :read, :time (long 40e9)}]))))))
  (testing "a power loss lasts until the power-on that started fnn"
    (is (= [{:kind :power, :node "n1", :seconds 20.0}
            {:kind :power, :node "n2", :seconds 50.0}]
           (map #(dissoc % :began :from :to)
                (closes/outages
                  (concat (nem 10 :power-off {"n1" {:store-bytes-before 9, :store-bytes-after 8}
                                              "n2" {:store-bytes-before 9, :store-bytes-after 9}})
                          (nem 30 :power-on {"n1" :started, "n2" "RPC didn't come back: timeout"})
                          (nem 60 :power-on {"n2" :started})))))))
  (testing "an outage op is one spell, as long as its node was down"
    (is (= [{:kind :outage, :node "n2", :seconds 150.0, :began (long 50e9), :from (long 50e9)
             :to (long 200e9)}]
           (closes/outages (nem 200 :outage {:node "n2", :duration-s 150, :down-ms 150000
                                             :caught []})))))
  (testing "a one-way partition cuts the pair too"
    (is (= [#{"n1" "n2"} #{"n1" "n3"}]
           (sort-by (comp vec sort)
                    (map :nodes (closes/outages
                                  (concat (nem 10 :start-partition [:isolated {"n1" #{"n2" "n3"}}])
                                          (nem 50 :stop-partition :network-healed)))))))))

(deftest candidates
  (testing "every spell over the deadline that touched the channel, the longest first"
    (is (= [60.0 40.0] (map :seconds ((closes/candidates faults-history 30) ["n1" "n2"])))))
  (testing "only the kinds that count"
    (is (= [] ((closes/candidates faults-history 30 #{:pause :partition}) ["n1" "n2"])))
    (is (= [:partition] (map :kind ((closes/candidates faults-history 30 #{:pause :partition})
                                    ["n3" "n4"]))))))

(deftest explainer
  (let [why (closes/explainer faults-history 30)]
    (is (= :kill (:kind (why ["n1" "n2"]))))            ; n1 down 60 s, n2 down 40 s: the longer
    (is (= 60.0 (:seconds (why ["n1" "n2"]))))
    (is (= :partition (:kind (why ["n3" "n4"]))))
    (is (nil? (why ["n3" "n5"])))                      ; n4's pause lasted 13 s
    (is (nil? ((closes/explainer faults-history 70) ["n3" "n4"])))))

(defn run-after
  "As `run`, with --force-close-ok-after 30 and `faults` before the final read."
  [final faults]
  (let [test {:nodes nodes, :topology :ring, :initial-channels (atom (ring))}]
    (checker/check (closes/checker 30) test
                   (concat faults
                           [{:type :ok, :f :probe, :value all-probes, :time (long 200e9)}
                            {:type :ok, :f :read, :value {:channels final, :payments []}
                             :time (long 201e9)}])
                   {})))

(deftest force-close-ok-after
  (let [closed {["0x23" "n2"] "Closed", ["0x23" "n3"] "Closed"}]
    (testing "a closed channel one of whose ends was down past the deadline is excused"
      (let [r (run-after (ring closed {}) (concat (nem 10 :kill {"n3" :killed})
                                                  (nem 45 :start {"n3" :started})))]
        (is (= true (:valid? r)))
        (is (= ["0x23"] (get-in r [:force-closed :channels])))
        (is (= {"0x23" {:kind :kill, :node "n3", :seconds 35.0}} (get-in r [:force-closed :because])))
        (is (= 30 (:force-close-ok-after-s r)))))
    (testing "but not after a shorter fault, nor one of other nodes"
      (doseq [faults [(concat (nem 10 :kill {"n3" :killed}) (nem 25 :start {"n3" :started}))
                      (concat (nem 10 :kill {"n1" :killed}) (nem 95 :start {"n1" :started}))
                      []]]
        (let [r (run-after (ring closed {}) faults)]
          (is (= false (:valid? r)))
          (is (= ["0x23"] (get-in r [:force-closed :not-excused])))
          (is (= #{"0x23"} (set (map :channel (:channel-problems r))))))))
    (testing "a partition of its two ends past the deadline"
      (let [r (run-after (ring closed {})
                         (concat (nem 10 :start-partition [:isolated {"n2" #{"n3" "n4"}
                                                                      "n3" #{"n1" "n2"}}])
                                 (nem 50 :stop-partition :network-healed)))]
        (is (= true (:valid? r)))
        (is (= {:kind :partition, :nodes ["n2" "n3"], :seconds 40.0}
               (get-in r [:force-closed :because "0x23"])))))))

(def local
  "fnn's state for the end that force-closed a channel."
  {:state_name "Closed", :state_flags "UNCOOPERATIVE_LOCAL|WAITING_ONCHAIN_SETTLEMENT"})

(def remote
  "fnn's state for the end that saw its counterparty's forced close on chain."
  {:state_name "Closed", :state_flags "UNCOOPERATIVE_REMOTE"})

(deftest a-close-one-end-has-not-noticed
  ; n3 is down from 10 s to 45 s, and n2 closes 0x23 on the deadline.
  (let [kill (concat (nem 10 :kill {"n3" :killed}) (nem 45 :start {"n3" :started}))]
    (testing "an end still ready, or one whose own forced close lost, isn't Closed: not judged"
      (doseq [other ["ChannelReady"
                     {:state_name "ShuttingDown", :state_flags "WAITING_COMMITMENT_CONFIRMATION"}]]
        (let [r (run-after (ring {["0x23" "n2"] local, ["0x23" "n3"] other} {}) kill)]
          (is (= false (:valid? r)))
          (is (= ["0x23"] (get-in r [:force-closed :not-settled])))
          (is (= "1" (:closes-not-settled r)))
          (is (= [] (get-in r [:force-closed :channels])))
          (is (nil? (get-in r [:force-closed :not-excused])))
          (is (nil? (:closes-not-excused r)))
          (is (= #{"0x23"} (set (map :channel (:channel-problems r))))))))
    (testing "once that end is Closed too, the rule judges the channel"
      (let [r (run-after (ring {["0x23" "n2"] local, ["0x23" "n3"] remote} {}) kill)]
        (is (= true (:valid? r)))
        (is (= ["0x23"] (get-in r [:force-closed :channels])))
        (is (nil? (get-in r [:force-closed :not-settled])))
        (is (nil? (:closes-not-settled r)))))
    (testing "an excused channel doesn't excuse another that isn't settled"
      (let [r (run-after (ring {["0x23" "n2"] local, ["0x23" "n3"] remote
                                ["0x34" "n4"] local}
                               {})
                         kill)]
        (is (= false (:valid? r)))
        (is (= ["0x23"] (get-in r [:force-closed :channels])))
        (is (= ["0x34"] (get-in r [:force-closed :not-settled])))
        (is (= #{"0x34"} (set (map :channel (:channel-problems r)))))))))

(deftest unsettled-ids
  (let [ids #(closes/unsettled-ids (closes/ends (ring) (ring % {})))]
    (is (= #{} (ids {})))
    (is (= #{"0x23"} (ids {["0x23" "n2"] local})))
    (is (= #{} (ids {["0x23" "n2"] local, ["0x23" "n3"] remote})))
    (testing "ends between ready and closed aren't a close yet"
      (is (= #{} (ids {["0x23" "n2"] "ShuttingDown", ["0x23" "n3"] "ShuttingDown"}))))))

(deftest settle-wait
  (let [closed  (ring {["0x23" "n2"] local, ["0x23" "n3"] remote} {})
        kill    (concat (nem 10 :kill {"n3" :killed}) (nem 45 :start {"n3" :started}))
        between [{:channel "0x23", :node "n3", :state {:state_name "ChannelReady"}}]
        settle  (fn [value] [{:type :invoke, :f :settle-channels, :time (long 150e9)}
                             {:type :ok, :f :settle-channels, :value value, :time (long 190e9)}])]
    (testing "the wait for the ends to agree is in the result"
      (let [r (run-after closed (concat kill (settle {:waited-s 40, :settled? true
                                                      :at-start {:between [], :disagree between}})))]
        (is (= true (:valid? r)))
        (is (= {:waited-s 40, :settled? true} (:settle-wait r)))))
    (testing "and so is what it gave up on"
      (let [r (run-after (ring {["0x23" "n2"] local} {})
                         (concat kill (settle {:waited-s 360, :settled? false
                                               :at-start {:between [], :disagree between}
                                               :at-end   {:between [], :disagree between}})))]
        (is (= false (:valid? r)))
        (is (= {:waited-s 360, :settled? false, :at-end {:between [], :disagree between}}
               (:settle-wait r)))
        (is (= ["0x23"] (get-in r [:force-closed :not-settled])))))
    (testing "but not a wait that found nothing to wait for, nor a run without one"
      (is (nil? (:settle-wait (run-after closed (concat kill (settle {:waited-s 0, :settled? true}))))))
      (is (nil? (:settle-wait (run-after closed kill)))))))

(deftest log-closes
  (let [lines [(str (at 1) "  INFO fnn: Starting node with git version abc")
               (inactive 40.25 "0x23")
               (broadcast 40.3 "0x23")
               ; A second line about the same channel changes nothing.
               (inactive 100 "0x23")
               ; The V2 deadline in channel maintenance logs no reason.
               (broadcast 70 "0x41")
               (str (at 80) " ERROR fnn::fiber::channel: Channel Hash256(0x12) from peer Pubkey(02ab) has been"
                    " reestablishing too long, shutting down it forcefully")
               (str "\u001b[2m" (at 81) "\u001b[0m \u001b[31mERROR\u001b[0m fnn::fiber::channel:"
                    " Channel Hash256(0x34) from peer Pubkey(02ab) cannot add or remove TLCs: its revocation"
                    " nonce ledger has been unbalanced for over 30000 ms (send_present=true,"
                    " verify_present=false) with no CommitmentSigned or RevokeAndAck from the peer,"
                    " shutting down it forcefully")
               (str (at 82) " ERROR fnn::fiber::channel: Channel 0x56 from peer Pubkey(02ab) has spent"
                    " over 300000 ms waiting for channel recovery, shutting down it forcefully")
               (str (at 83) " ERROR fnn::fiber::channel: Failed to verify commitment_signed message:"
                    " InvalidParameter(\"bad\"), shutdown channel Hash256(0x78) forcefully")
               ; A cooperative close is not a forced one.
               (str (at 90) "  INFO fnn::fiber::network: Channel (Hash256(0x99)) to peer Pubkey(02ab) is"
                    " closed Byte32(0x77). Broadcasting closing transaction (\"cooperatively\") now.")]]
    (is (= {"0x23" {:at-ms (+ epoch-ms 40250), :reason :peer-deadline}
            "0x41" {:at-ms (+ epoch-ms 70000), :reason :silent}
            "0x12" {:at-ms (+ epoch-ms 80000), :reason :reestablish-timeout}
            "0x34" {:at-ms (+ epoch-ms 81000), :reason :nonce-ledger}
            "0x56" {:at-ms (+ epoch-ms 82000), :reason :reestablish-timeout}
            "0x78" {:at-ms (+ epoch-ms 83000), :reason :commitment-signed}}
           (closes/log-closes lines)))))

(deftest wall-clock
  (is (nil? (closes/wall-origin-ms faults-history)))
  (is (= epoch-ms (closes/wall-origin-ms (wall faults-history))))
  (is (false? (closes/clocks-moved? {:faults [:kill :partition]})))
  (is (true? (closes/clocks-moved? {:faults [:kill :clock]})))
  (is (true? (closes/clocks-moved? {:faults [:outage], :outage-jump 34200}))))

(defn run-logs
  "As `run-after`, with the wall clock in the history and `logs`, {node lines},
  as the nodes' fnn logs. `test-opts` adds to the test map; `opts` to the
  rule's options."
  [final faults logs & [test-opts opts]]
  (let [test (merge {:nodes nodes, :topology :ring, :initial-channels (atom (ring))} test-opts)]
    (checker/check (closes/checker (merge {:seconds     30
                                           :read-closes (fn [_ node]
                                                          (some-> (get logs node) closes/log-closes))}
                                          opts))
                   test
                   (concat (wall faults)
                           [{:type :ok, :f :probe, :value all-probes, :time (long 400e9)}
                            {:type :ok, :f :read, :value {:channels final, :payments []}
                             :time (long 401e9)}])
                   {})))

(deftest what-the-logs-say
  (let [closed (ring {["0x23" "n2"] "Closed", ["0x23" "n3"] "Closed"} {})
        ; n3 is down from 100 s to 160 s.
        kill   (concat (nem 100 :kill {"n3" :killed}) (nem 160 :start {"n3" :started}))]
    (testing "the peer closed once the outage had lasted the deadline"
      (let [r (run-logs closed kill {"n2" [(inactive 129 "0x23") (broadcast 129.1 "0x23")], "n3" []})]
        (is (= true (:valid? r)))
        (is (= {"0x23" {:kind :kill, :node "n3", :seconds 60.0
                        :closed-by "n2", :closed-at-s 129.0, :logged :peer-deadline}}
               (get-in r [:force-closed :because])))
        (is (= "1: kill 1" (:closes-excused r)))
        (is (nil? (get-in r [:force-closed :logs-not-used])))))
    (testing "the node that was down closed when it came back, without a reason logged"
      (let [r (run-logs closed kill {"n2" [], "n3" [(broadcast 215 "0x23")]})]
        (is (= true (:valid? r)))
        (is (= {:closed-by "n3", :closed-at-s 215.0, :logged :silent}
               (select-keys (get-in r [:force-closed :because "0x23"])
                            [:closed-by :closed-at-s :logged])))))
    (testing "a close before the outage, or before it had lasted the deadline, isn't its doing"
      (doseq [t [50 110 120]]
        (let [r (run-logs closed kill {"n2" [(inactive t "0x23")]})]
          (is (= false (:valid? r)))
          (is (= ["0x23"] (get-in r [:force-closed :not-excused])))
          (is (= :outside-the-outages (get-in r [:force-closed :not-excused-why "0x23" :why])))
          (is (= [{:kind :kill, :node "n3", :seconds 60.0, :from-s 99.5, :to-s 160.0}]
                 (get-in r [:force-closed :not-excused-why "0x23" :outages])))
          (is (= "1: outside-the-outages 1" (:closes-not-excused r))))))
    (testing "nor is one long after the node came back"
      (let [r (run-logs closed kill {"n3" [(inactive 260 "0x23")]})]
        (is (= false (:valid? r)))
        (is (= :outside-the-outages (get-in r [:force-closed :not-excused-why "0x23" :why])))))
    (testing "a close fnn gave another reason for isn't the peer deadline"
      (let [line (str (at 135) " ERROR fnn::fiber::channel: Channel Hash256(0x23) from peer Pubkey(02ab) cannot"
                      " add or remove TLCs: its revocation nonce ledger has been unbalanced for over"
                      " 30000 ms (send_present=true, verify_present=false) with no CommitmentSigned"
                      " or RevokeAndAck from the peer, shutting down it forcefully")
            r    (run-logs closed kill {"n2" [line (broadcast 135.1 "0x23")]})]
        (is (= false (:valid? r)))
        (is (= {:why :another-reason, :closed-by "n2", :closed-at-s 135.0, :logged :nonce-ledger}
               (get-in r [:force-closed :not-excused-why "0x23"])))))
    (testing "the first end to close decides: what the other end logged later doesn't count"
      (let [late (str (at 300) " ERROR fnn::fiber::channel: Channel Hash256(0x23) from peer Pubkey(02ab) has"
                      " been reestablishing too long, shutting down it forcefully")
            r    (run-logs closed kill {"n2" [(inactive 130 "0x23")], "n3" [late]})]
        (is (= true (:valid? r)))
        (is (= "n2" (get-in r [:force-closed :because "0x23" :closed-by])))))
    (testing "without a close in any log, the outage alone decides"
      (let [r (run-logs closed kill {"n2" [], "n3" nil})]
        (is (= true (:valid? r)))
        (is (= {"0x23" {:kind :kill, :node "n3", :seconds 60.0}} (get-in r [:force-closed :because])))))
    (testing "and so it does when a fault moved fnn's clock"
      (let [r (run-logs closed kill {"n2" [(inactive 50 "0x23")]} {:faults [:kill :clock]})]
        (is (= true (:valid? r)))
        (is (= :clocks-moved (get-in r [:force-closed :logs-not-used])))))
    (testing "a channel no outage touched isn't excused, and the logs say who closed it"
      (let [r (run-logs closed (concat (nem 100 :kill {"n1" :killed}) (nem 160 :start {"n1" :started}))
                        {"n2" [(inactive 129 "0x23")]})]
        (is (= false (:valid? r)))
        (is (= {:why :no-outage, :closed-by "n2", :closed-at-s 129.0, :logged :peer-deadline}
               (get-in r [:force-closed :not-excused-why "0x23"])))))
    (testing "only the kinds asked for count"
      (let [logs {"n2" [(inactive 129 "0x23")]}
            r    (run-logs closed kill logs {} {:kinds #{:pause :partition}})]
        (is (= false (:valid? r)))
        (is (= :no-outage (get-in r [:force-closed :not-excused-why "0x23" :why])))
        (is (= [:partition :pause] (:force-close-ok-kinds r)))
        (is (= true (:valid? (run-logs closed kill logs {} {:kinds #{:kill}}))))))))

(deftest force-close-ok-counts
  (let [closed {["0x23" "n2"] "Closed", ["0x23" "n3"] "Closed"}]
    (is (= "1" (:closes-excused (run (ring closed {}) []
                                     (mapv #(if (#{#{"n2" "n3"}} (set [(:from %) (:to %)]))
                                              (assoc % :status "Failed")
                                              %)
                                           all-probes)))))
    (is (nil? (:closes-excused (run (ring) [] all-probes))))))

;; Availability after an excused close

(defn- pays
  "`n` payments from `from` to `to` in `window`, each completing as `type`,
  starting at second `t`."
  [window from to n type t]
  (mapcat (fn [i]
            (let [op {:f :pay, :process (+ 100 i), :window window, :value {:id (+ (* 1000 t) i)}}]
              [(assoc op :type :invoke, :time (long (* (+ t (* 0.1 i)) 1e9)))
               (cond-> (-> op
                           (assoc :type type, :time (long (* (+ t 0.05 (* 0.1 i)) 1e9)))
                           (update :value assoc :from from, :to to))
                 (not= :ok type) (assoc :error "Failed to build route"))]))
          (range n)))

(defn- line
  "The setup channels of a line n1 - n2 - n3: {node [channel ...]}."
  []
  {"n1" [(channel "0x12" 1 1)]
   "n2" [(channel "0x12" 1 1) (channel "0x23" 1 1)]
   "n3" [(channel "0x23" 1 1)]})

(defn- windows
  "The two windows' payments of `history`, as closes/availability takes them."
  [history]
  [(#'jepsen.fiber.checker/window-pays history :baseline)
   (#'jepsen.fiber.checker/window-pays history :recovery)])

(deftest setup-graph
  (is (= {"n1" #{"n2"}, "n2" #{"n1" "n3"}, "n3" #{"n2"}} (closes/setup-graph (line) #{})))
  (is (= {"n2" #{"n3"}, "n3" #{"n2"}} (closes/setup-graph (line) #{"0x12"})))
  (is (= {} (closes/setup-graph (line) #{"0x12" "0x23"}))))

(deftest availability-after-an-excused-close
  (let [before (concat (pays :baseline "n1" "n3" 10 :ok 0)
                       (pays :baseline "n3" "n1" 10 :ok 2)
                       (pays :baseline "n2" "n3" 10 :ok 4))
        after  (fn [type] (concat (pays :recovery "n1" "n3" 10 :fail 100)
                                  (pays :recovery "n3" "n1" 10 :fail 102)
                                  (pays :recovery "n2" "n3" 10 type 104)))
        verdict (fn [cut history]
                  (let [[b r] (windows history)]
                    (closes/availability 5 (line) cut b r)))]
    (testing "the pairs an excused close cut off are left out of both windows"
      (let [r (verdict #{"0x12"} (concat before (after :ok)))]
        (is (= true (:valid? r)))
        (is (= "100.0% -> 100.0%" (:success-rates r)))
        (is (= 10 (get-in r [:baseline :payments]) (get-in r [:recovery :payments])))
        (is (= {["n1" "n3"] {:baseline 10, :recovery 10}
                ["n3" "n1"] {:baseline 10, :recovery 10}}
               (:cut-off r)))))
    (testing "a pair that still has a path is judged as ever"
      (let [r (verdict #{"0x12"} (concat before (after :fail)))]
        (is (= false (:valid? r)))
        (is (= "100.0% -> 0.0%" (:success-rates r)))))
    (testing "without an excused close nothing is left out"
      (let [r (verdict #{} (concat before (after :ok)))]
        (is (= false (:valid? r)))
        (is (= 30 (get-in r [:recovery :payments])))
        (is (nil? (:cut-off r)))))
    (testing "every pair cut off: nothing to compare"
      (let [r (verdict #{"0x12" "0x23"} (concat before (after :fail)))]
        (is (= true (:valid? r)))
        (is (= true (:nothing-to-compare r)))
        (is (= 0 (get-in r [:recovery :payments])))
        (is (= 3 (count (:cut-off r))))))
    (testing "too few payments left to compare is unknown, as in the plain check"
      (let [r (verdict #{"0x12"} (concat before
                                         (pays :recovery "n1" "n3" 10 :fail 100)
                                         (pays :recovery "n2" "n3" 3 :ok 104)))]
        (is (= :unknown (:valid? r)))
        (is (nil? (:nothing-to-compare r)))))))

(deftest availability-checker
  (let [closed  [(channel "0x12" 1 1 "Closed" [])]
        initial {"n1" [(channel "0x12" 1 1)], "n2" [(channel "0x12" 1 1)]}
        check   (fn [chk kill-s final]
                  (checker/check chk
                                 {:nodes ["n1" "n2"], :initial-channels (atom initial)}
                                 (concat (pays :baseline "n1" "n2" 20 :ok 0)
                                         (nem 10 :kill {"n2" :killed})
                                         (nem (+ 10 kill-s) :start {"n2" :started})
                                         (pays :recovery "n1" "n2" 20 :fail 100)
                                         [{:type :ok, :f :read, :time (long 201e9)
                                           :value {:channels final, :payments []}}])
                                 {}))
        ends    {"n1" closed, "n2" closed}]
    (testing "the only channel closed after a kill past the deadline: nothing to compare"
      (let [r (check (closes/availability-checker 5 30) 35 ends)]
        (is (= true (:valid? r)))
        (is (= true (:nothing-to-compare r)))
        (is (= 1 (:excused-closes r)))
        (is (= {["n1" "n2"] {:baseline 20, :recovery 20}} (:cut-off r)))))
    (testing "a close the rule doesn't excuse leaves the check as it was"
      (let [r (check (closes/availability-checker 5 30) 20 ends)]
        (is (= false (:valid? r)))
        (is (= "100.0% -> 0.0%" (:success-rates r)))
        (is (nil? (:cut-off r)))))
    (testing "--force-close-ok excuses every closed channel"
      (is (= true (:valid? (check (closes/availability-checker 5) 20 ends)))))
    (testing "no channel closed: the plain check"
      (let [r (check (closes/availability-checker 5 30) 35 initial)]
        (is (= false (:valid? r)))
        (is (nil? (:excused-closes r)))))))
