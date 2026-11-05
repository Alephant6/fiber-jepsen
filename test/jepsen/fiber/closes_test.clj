(ns jepsen.fiber.closes-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.fiber [closes :as closes]
                          [rpc :as rpc]
                          [topology :as topology]]))

(defn channel
  ([id local remote] (channel id local remote "ChannelReady" []))
  ([id local remote state tlcs]
   {:channel_id     id
    :state          {:state_name state}
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
        (is (= [] (get-in r [:force-closed :channels])))))
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
         (sort-by :seconds (map #(dissoc % :from :to) (closes/outages faults-history)))))
  (testing "a spell nothing ends lasts until the history does"
    (is (= [{:kind :kill, :node "n2", :seconds 30.0}]
           (map #(dissoc % :from :to)
                (closes/outages (concat (nem 10 :kill {"n2" :killed})
                                        [{:type :ok, :process 0, :f :read, :time (long 40e9)}])))))))

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
