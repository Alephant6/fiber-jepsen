(ns jepsen.fiber.schedule-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [client :as client]
                          [schedule :as schedule]]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :rate 5, :time-limit 60, :nemesis-interval 15
   :kill-targets [:one :all], :faults [:kill :pause :partition :disconnect]})

(defn simulate
  "Runs `g` in Jepsen's generator simulator with three clients and a nemesis,
  each operation taking (latency op) nanoseconds. Returns the invocations."
  [g latency]
  (->> (gt/simulate (gen/context {:concurrency 3})
                    g
                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + (latency op)))))
       ; Client invocations are :invoke; the nemesis's are :info, and every
       ; completion here is :ok.
       (filter #(#{:invoke :info} (:type %)))))

(def fast (constantly 1000000))

(defn slow
  [op]
  (* 1000000 (+ 50 (mod (hash (:value op)) 400))))

(defn values
  "[f value] of each invocation whose :f is in fs."
  [fs history]
  (->> history (filter (comp fs :f)) (map (juxt :f :value))))

(def streams
  "The :f values of each stream: payments and one per nemesis package."
  [#{:pay}
   #{:start-partition :stop-partition}
   #{:kill :start :pause :resume}
   #{:disconnect :reconnect}])

(defn common-prefix
  "The number of leading elements xs and ys share, and the shorter length."
  [xs ys]
  [(count (take-while true? (map = xs ys))) (min (count xs) (count ys))])

(defn workload
  "Payments and every fault, with a fault every 2 s on average, for 400 ops."
  [seed]
  (let [streams (schedule/streams seed)
        opts    (assoc opts :nemesis-interval 2)]
    (gen/limit 400 (gen/nemesis (schedule/faults streams opts)
                                (fiber/payments opts streams)))))

(deftest same-seed-same-operations
  (testing "the same seed draws the same payments and faults"
    (is (= (take 50 (schedule/payments (:pay (schedule/streams 7)) 3))
           (take 50 (schedule/payments (:pay (schedule/streams 7)) 3))))
    (is (not= (take 50 (schedule/payments (:pay (schedule/streams 7)) 3))
              (take 50 (schedule/payments (:pay (schedule/streams 8)) 3)))))

  (testing "timing changes when operations run, not which ones"
    (let [a (simulate (workload 7) fast)
          b (simulate (workload 7) slow)]
      ; How the streams interleave depends on timing, so each run holds a
      ; different number of each stream's ops; the shorter run's must be a
      ; prefix of the longer one's.
      (doseq [fs streams]
        (let [[same shorter] (common-prefix (values fs a) (values fs b))]
          (is (< 10 shorter) (str fs))
          (is (= shorter same) (str fs))))
      (testing "while the times differ"
        (is (not= (map :time a) (map :time b)))))))

(deftest payments
  (let [ps (take 200 (schedule/payments (:pay (schedule/streams 1)) 3))]
    (is (every? #(<= 1000000 (:amount (:value %)) 20000000) ps))
    (is (= #{0 1} (set (map (comp :to-index :value) ps))))
    (is (= 200 (count (set (map (comp :id :value) ps)))))))

(deftest destination
  (let [test {:nodes nodes}]
    (is (= "n1" (client/destination test "n2" {:value {:to-index 0}})))
    (is (= "n3" (client/destination test "n2" {:value {:to-index 1}})))
    (is (= "n3" (client/destination test "n1" {:value {:to "n3", :to-index 0}})))
    (testing "without :to-index, any other node"
      (is (#{"n1" "n3"} (client/destination test "n2" {:value {}}))))))

(deftest faults
  (let [streams (schedule/streams 3)]
    (testing "kills and pauses each alternate with their recovery, on named nodes"
      (let [ops (take 200 (schedule/db-faults (:db streams) nodes
                                              {:kill? true, :pause? true
                                               :kill-targets [:one :all]
                                               :pause-targets [:one]}))]
        (doseq [[fault recovery] [[:kill :start] [:pause :resume]]]
          (let [fs (map :f (filter (comp #{fault recovery} :f) ops))]
            (is (= (take (count fs) (cycle [fault recovery])) fs))))
        (is (every? #(or (= nodes %) (and (= 1 (count %)) (some #{(first %)} nodes)))
                    (map :value (filter (comp #{:kill :pause} :f) ops))))))

    (testing "a partition isolates one node"
      (let [grudges (->> (schedule/partitions (:partition streams) nodes)
                         (take 20)
                         (filter (comp #{:start-partition} :f))
                         (map :value))]
        (is (seq grudges))
        (doseq [g grudges]
          (let [loner (some (fn [[node others]] (when (= 2 (count others)) node)) g)]
            (is loner)
            (is (= #{loner} (set (mapcat val (dissoc g loner)))))))))

    (testing "a disconnect names one channel's peers"
      (is (every? #{["n1" "n2"] ["n2" "n3"]}
                  (->> (schedule/disconnects (:disconnect streams) nodes)
                       (take 20)
                       (filter (comp #{:disconnect} :f))
                       (map :value)))))

    (testing "no faults, no generator"
      (is (nil? (schedule/faults streams (assoc opts :faults [])))))))

(deftest seed
  (is (= 42 (:seed (fiber/fiber-test (assoc opts :seed 42)))))
  (is (integer? (:seed (fiber/fiber-test opts)))))
