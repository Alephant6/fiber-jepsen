(ns jepsen.fiber.schedule-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [client :as client]
                          [faults :as faults]
                          [schedule :as schedule]]
            [jepsen.fiber.faults [disconnect :as disconnect]
                                 [partition :as partition]
                                 [process :as process]]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :rate 5, :time-limit 60, :nemesis-interval 15
   :kill-targets [:one :all], :faults [:kill :pause :partition :disconnect :packet]})

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
   #{:disconnect :reconnect}
   #{:start-packet :stop-packet}])

(defn common-prefix
  "The number of leading elements xs and ys share, and the shorter length."
  [xs ys]
  [(count (take-while true? (map = xs ys))) (min (count xs) (count ys))])

(defn workload
  "Payments and every fault, with a fault every 2 s on average, for 400 ops."
  [seed]
  (let [streams (schedule/streams seed)
        opts    (assoc opts :nemesis-interval 2)]
    (gen/limit 400 (gen/nemesis (faults/generator streams opts)
                                (fiber/payments opts streams)))))

(deftest the-fault-window-lasts-the-time-limit
  ; The seed of a CI run of --scenario c-08 --time-limit 90 whose window ended
  ; after 1.3 s: its first gap between faults is 100 s, so the resume after the
  ; first pause falls after the limit.
  (let [opts    (assoc opts :faults [:pause] :nemesis-interval 30 :time-limit 90)
        streams (schedule/streams 195254960918858)
        pays    (->> (simulate (fiber/fault-window opts streams)
                               (constantly 200000000))
                     (filter #(= :pay (:f %))))]
    (is (< 89e9 (:time (last pays))))
    (is (< 400 (count pays)))))

(deftest the-healing-is-due-at-the-limit
  ; Partitions of 5-15 s, 20 s apart on average, for 60 s, and payments that
  ; take 45 s each, so every client is busy until 30 s after the limit.
  (let [opts  (assoc opts :faults [:partition] :partition-duration [5 15]
                     :nemesis-interval 20 :time-limit 60)
        heal  {:type :info, :f :heal}
        slow  #(if (= :pay (:f %)) 45000000000 1000000)
        run   (fn [window seed]
                (simulate (gen/phases (window (schedule/streams seed))
                                      (gen/nemesis {:type :info, :f :next-phase}))
                          slow))
        at    (fn [ops f] (:time (first (filter #(= f (:f %)) ops))))
        cuts  (fn [ops]
                ; How long each partition lasted, in seconds: until the nemesis's
                ; next operation, which ends it.
                (for [[a b] (partition 2 1 (filter #(= :nemesis (:process %)) ops))
                      :when (= :start-partition (:f a))]
                  (/ (- (:time b) (:time a)) 1e9)))]
    (testing "the healing comes at the limit, whatever the clients are doing"
      (doseq [seed (range 1 31)]
        (let [ops   (run #(fiber/fault-window opts % [heal]) seed)
              start (:time (first (filter #(= :nemesis (:process %)) ops)))]
          (is (<= (+ start 60e9) (at ops :heal) (+ start 60.1e9)) (str "seed " seed))
          (is (every? #(<= % 15.1) (cuts ops)) (str "seed " seed))
          (testing "and the next phase still waits for the payments in flight"
            (is (< (at ops :heal) (at ops :next-phase)))
            (is (= 1 (count (filter #(= :heal (:f %)) ops))))))))
    (testing "as a phase of its own, it waited for them, and the last partition lasted on"
      (let [phased (fn [streams] (gen/phases (fiber/fault-window opts streams)
                                             (gen/nemesis heal)))
            long   (for [seed (range 1 31)
                         c    (cuts (run phased seed))
                         :when (< 15.1 c)]
                     c)]
        (is (seq long))
        (is (some #(< 30 %) long))))
    (testing "without faults there is nothing to heal, and the payments keep their limit"
      (let [ops (run #(fiber/fault-window (assoc opts :faults []) % []) 1)]
        (is (nil? (at ops :heal)))
        (is (< 44e9 (:time (last (filter #(= :pay (:f %)) ops))) 60e9))))))

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
      (let [ops (take 200 (process/ops (:db streams) nodes
                                       {:kill? true, :pause? true
                                        :kill-targets [:one :all]
                                        :pause-targets [:one]}))]
        (doseq [[fault recovery] [[:kill :start] [:pause :resume]]]
          (let [fs (map :f (filter (comp #{fault recovery} :f) ops))]
            (is (= (take (count fs) (cycle [fault recovery])) fs))))
        (is (every? #(or (= nodes %) (and (= 1 (count %)) (some #{(first %)} nodes)))
                    (map :value (filter (comp #{:kill :pause} :f) ops))))))

    (testing "a partition isolates one node"
      (let [grudges (->> (partition/ops (:partition streams) nodes)
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
                  (->> (disconnect/ops (:disconnect streams) {:nodes nodes})
                       (take 20)
                       (filter (comp #{:disconnect} :f))
                       (map :value)))))

    (testing "no faults, no generator"
      (is (nil? (faults/generator streams (assoc opts :faults [])))))))

(deftest seed
  (is (= 42 (:seed (fiber/fiber-test (assoc opts :seed 42)))))
  (is (integer? (:seed (fiber/fiber-test opts)))))
