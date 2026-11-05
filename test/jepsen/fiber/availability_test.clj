(ns jepsen.fiber.availability-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber.checker :as fc]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :rate 5, :time-limit 60, :quiesce 30, :nemesis-interval 10
   :kill-targets [:one :all], :faults [:kill :disconnect], :seed 20261105})

(defn- pay
  "An invocation and its completion, `ms` apart, on process `p` at second `t`."
  [window p t type ms]
  (let [op {:f :pay, :process p, :window window, :value {:id (+ (* 1000 p) t)}}]
    [(assoc op :type :invoke, :time (long (* t 1e9)))
     (assoc op :type type, :time (long (+ (* t 1e9) (* ms 1e6))))]))

(deftest window-stats
  (let [history (concat (pay :baseline 0 0 :ok 100)
                        (pay :baseline 1 0 :ok 300)
                        (pay :baseline 0 1 :fail 50)
                        (pay nil 1 2 :ok 10)
                        (pay :baseline 1 3 :info 15000)
                        (pay :recovery 0 20 :ok 200))
        pairs   (fc/window-pays history :baseline)
        stats   (fc/window-stats pairs)]
    (testing "only the window's payments, each paired with its invocation"
      (is (= 4 (count pairs)))
      (is (every? (fn [[i c]] (= (:value i) (:value c))) pairs)))
    (is (= {:payments 4, :ok 2, :failed 1, :no-answer 1, :success-pct 50.0}
           (select-keys stats [:payments :ok :failed :no-answer :success-pct])))
    (testing "throughput and latency, over the window's first invocation to its last completion"
      (is (= 18.0 (:seconds stats)))
      (is (= 0.11 (:ok-per-s stats)))
      (is (= {:p50 300.0, :p95 300.0, :max 300.0} (:latency-ms stats))))
    (testing "unsuccessful payments by sender and by error"
      (let [err  "Failed to build route, Insufficient balance: max outbound liquidity 0 is insufficient, required amount: 8000000"
            bad  (fn [p from type error]
                   (let [[i c] (pay :baseline p 0 type 10)]
                     [(assoc-in i [:value :from] from)
                      (-> c (assoc-in [:value :from] from) (assoc :error error))]))
            s    (fc/window-stats (fc/window-pays (concat (bad 0 "n3" :fail err)
                                                          (bad 1 "n3" :fail (str/replace err "8000000" "20000"))
                                                          (bad 2 "n1" :info [:no-reply "SocketTimeoutException"]))
                                                  :baseline))]
        (is (= {"n1" 1, "n3" 2} (get-in s [:unsuccessful :by-sender])))
        (is (= [["Failed to build route, Insufficient balance: max outbound liquidity N is insufficient, required amount: N" 2]
                ["[:no-reply \"SocketTimeoutException\"]" 1]]
               (get-in s [:unsuccessful :by-error])))))
    (testing "an empty window"
      (is (= 0 (:payments (fc/window-stats []))))
      (is (nil? (:success-pct (fc/window-stats [])))))))

(defn- window
  [n ok]
  {:payments n, :success-pct (* 100.0 (/ ok n))})

(deftest verdict
  (is (true? (:valid? (fc/availability 5 (window 100 98) (window 100 94)))))
  (is (true? (:valid? (fc/availability 5 (window 100 90) (window 100 99)))))
  (testing "more than the tolerance below the baseline"
    (let [r (fc/availability 5 (window 100 98) (window 100 90))]
      (is (false? (:valid? r)))
      (is (= 8.0 (:drop-points r)))
      (is (= "98.0% -> 90.0%" (:success-rates r)))))
  (testing "too few payments to tell"
    (is (= :unknown (:valid? (fc/availability 5 (window 5 5) (window 100 10)))))
    (is (= :unknown (:valid? (fc/availability 5 (window 100 98) {:payments 0}))))))

(defn simulate
  "The ops the whole test generator issues for `opts`, each taking 50 ms. The
  simulator doesn't wait for :sleep ops, but keeps them in order."
  [opts]
  (->> (gt/simulate (gen/context {:concurrency 3})
                    (:generator (fiber/fiber-test opts))
                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 50000000))))
       (filter #(#{:invoke :info :sleep} (:type %)))))

(deftest windows
  (let [plain  (simulate opts)
        with   (simulate (assoc opts :availability-window 20))
        pays   (fn [h w] (filter #(and (= :pay (:f %)) (= w (:window %))) h))
        ; The nemesis's ops, but the health sample at the very end.
        faults (filter #(and (= :info (:type %)) (not= :health (:f %))) with)]
    (testing "without the option the generator has no windows"
      (is (empty? (filter :window plain))))
    (testing "the baseline comes before the first fault, the recovery after the healing and --quiesce"
      (let [base (pays with :baseline)
            rec  (pays with :recovery)
            idx  (fn [pred] (count (take-while (complement pred) with)))]
        (is (< 80 (count base) 120))
        (is (< 80 (count rec) 120))
        (is (< (idx #{(last base)}) (idx #{(first faults)})))
        (testing "the healing, then --quiesce, then the recovery, then the probe"
          (is (< (idx #{(last faults)}) (idx #(= :sleep (:type %))) (idx #{(first rec)})))
          (is (= 30 (:value (first (filter #(= :sleep (:type %)) with)))))
          (is (< (idx #{(last rec)}) (idx #(= :probe (:f %))))))))
    (testing "the fault window's payments and faults are the same as without the option"
      (let [vals (fn [h] (map (juxt :f :value) (remove :window (filter #(#{:pay :kill :start :disconnect :reconnect} (:f %)) h))))
            a    (vals plain)
            b    (vals with)
            n    (min (count a) (count b))]
        (is (< 250 n))
        (is (= (take n a) (take n b)))))
    (testing "not with --close"
      (is (empty? (filter :window (simulate (assoc opts :availability-window 20 :close true))))))))
