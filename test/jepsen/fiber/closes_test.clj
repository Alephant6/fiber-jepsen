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
