(ns jepsen.fiber.faults-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber [faults :as faults]
                          [schedule :as schedule]
                          [topology :as topology]]
            [jepsen.fiber.faults.packet :as packet]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :nemesis-interval 2, :kill-targets [:one :all]
   :packet-targets [:one :all :chain], :packet-delay [500 3000], :packet-loss 10})

(defn fault-ops
  "The nemesis ops of the fault generator for `faults`, under Jepsen's simulator."
  [seed faults]
  (->> (gt/simulate (gen/context {:concurrency 1})
                    (gen/nemesis (gen/limit 200 (faults/generator (schedule/streams seed)
                                                                  (assoc opts :faults faults))))
                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 1000000))))
       (filter #(= :info (:type %)))
       (map (juxt :f :value))))

(deftest registry
  (testing "every fault declares what the registry needs"
    (doseq [f faults/registry]
      (is (seq (:faults f)))
      (is (keyword? (:stream f)))
      (is (fn? (:package f)))
      (is (fn? (:ops f)))))
  (testing "no two faults share a --faults name or a stream"
    (is (apply distinct? (mapcat :faults faults/registry)))
    (is (apply distinct? (map :stream faults/registry))))
  (is (= #{:kill :pause :partition :disconnect :packet :disk} faults/supported))
  (testing "only the faults asked for"
    (is (= [:partition :db] (map :stream (faults/in-use {:faults [:kill :partition]}))))
    (is (empty? (faults/in-use {:faults []})))))

(deftest streams
  (let [s (schedule/streams 7)]
    (testing "the first streams are split off the seeded root, as before"
      (is (identical? (:pay s) (schedule/stream s :pay))))
    (testing "a new stream depends on the seed and its name"
      (let [draw #(.nextLong (schedule/stream %1 %2))]
        (is (= (draw (schedule/streams 7) :packet) (draw (schedule/streams 7) :packet)))
        (is (not= (draw (schedule/streams 7) :packet) (draw (schedule/streams 8) :packet)))
        (is (not= (draw (schedule/streams 7) :packet) (draw (schedule/streams 7) :clock)))))))

(deftest adding-a-fault-changes-no-other
  (let [only (fn [fs ops] (filter (comp fs first) ops))
        kill #{:kill :start}]
    (doseq [seed [1 2 3]]
      (let [alone (only kill (fault-ops seed [:kill]))
            mixed (only kill (fault-ops seed [:kill :packet]))
            n     (min (count alone) (count mixed))]
        (is (< 10 n))
        (is (= (take n alone) (take n mixed)))))))

(deftest packet
  (let [ops    (take 400 (packet/ops (schedule/stream (schedule/streams 5) :packet) opts))
        starts (map :value (filter (comp #{:start-packet} :f) ops))]
    (testing "each fault is followed by its healing"
      (is (= (take 400 (cycle [:start-packet :stop-packet])) (map :f ops))))
    (testing "targets come from the specs asked for"
      (is (= #{[:one] [:all] [:chain]}
             (set (for [[targets _] starts]
                    (cond (= nodes targets)            [:all]
                          (= ["chain"] targets)        [:chain]
                          (= 1 (count targets))        [:one]))))))
    (testing "delays are drawn from the range, loss is as asked"
      (doseq [[_ b] starts]
        (is (<= 500 (parse-long (re-find #"\d+" (get-in b [:delay :time]))) 3000))
        (is (= "10%" (get-in b [:loss :percent])))))
    (testing "no loss unless asked"
      (is (nil? (:loss (packet/behaviors 800 0)))))
    (is (= [500 3000] (packet/parse-delay "500-3000")))
    (is (= [800 800] (packet/parse-delay "800")))))

(deftest topologies
  (is (= [["n1" "n2"] ["n2" "n3"]] (topology/edges {:nodes nodes})))
  (is (= [["n1" "n2"] ["n2" "n3"]] (topology/edges {:nodes nodes, :topology :line}))))
