(ns jepsen.fiber.process-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [faults :as faults]
                          [schedule :as schedule]]
            [jepsen.fiber.faults.process :as process]))

(def nodes ["n1" "n2" "n3"])

(defn kill-ops
  "The first `n` ops of the kill fault for `opts` and seed 11."
  [n opts]
  (take n ((:ops process/fault) (schedule/stream (schedule/streams 11) :db)
                                (merge {:nodes nodes, :faults [:kill], :kill-targets [:one :all]}
                                       opts))))

(deftest middle-target
  (is (= ["n2"] (process/middle-nodes {:nodes nodes})))
  (is (= [] (process/middle-nodes {:nodes ["n1" "n2"]})))
  (testing "every kill names the middle node"
    (let [kills (filter #(= :kill (:f %)) (kill-ops 100 {:kill-targets [:middle]}))]
      (is (< 40 (count kills)))
      (is (every? #(= ["n2"] (:value %)) kills))))
  (testing "mixed with other specs"
    (is (= #{["n2"] nodes}
           (set (map :value (filter #(= :kill (:f %))
                                    (kill-ops 100 {:kill-targets [:middle :all]})))))))
  (testing "a topology without a middle node is an error up front"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"no node in topology line"
                          (kill-ops 1 {:nodes ["n1" "n2"], :kill-targets [:middle]})))))

(deftest the-signal-changes-no-op
  (is (= (kill-ops 200 {}) (kill-ops 200 {:kill-signal :term, :term-grace 5}))))

(defn- kill
  "A :kill completion at second `t` with these per-node results."
  [t results]
  {:type :info, :process :nemesis, :f :kill, :time (long (* t 1e9)), :value results})

(deftest exit-check
  (let [check   #(checker/check (process/exit-checker 30) {} % {})
        history [{:type :info, :process :nemesis, :f :kill, :value ["n1"]}
                 (kill 10 {"n1" {:signal "TERM", :exit-ms 420}})
                 (kill 40 {"n1" {:signal "TERM", :exit-ms 900}
                           "n2" {:signal "TERM", :exit-ms 130}
                           "n3" :not-running})]]
    (testing "every TERM exited in time"
      (let [r (check history)]
        (is (true? (:valid? r)))
        (is (= 3 (:terms r) (:exited r)))
        (is (= {:median 420, :max 900} (:exit-ms r)))
        (is (= [{:node "n1", :time 40000000000, :exit-ms 900}] (take 1 (:slowest r))))))
    (testing "one needed SIGKILL"
      (let [r (check (conj history (kill 70 {"n2" {:signal "TERM", :exit-ms nil
                                                  :killed-after-ms 30100}})))]
        (is (false? (:valid? r)))
        (is (= [{:node "n2", :time 70000000000, :killed-after-ms 30100}] (:not-graceful r)))))
    (testing "an exit fnn didn't log is reported"
      (let [r (check (conj history (kill 70 {"n3" {:signal "TERM", :exit-ms 80, :logged? false}})))]
        (is (true? (:valid? r)))
        (is (= [{:node "n3", :time 70000000000}] (:unlogged r)))))
    (testing "no TERM at all"
      (is (= :unknown (:valid? (check [(kill 10 {"n1" nil})])))))))

(deftest checkers
  (is (contains? (faults/checkers {:faults [:kill], :kill-signal :term}) :term-exits))
  (is (empty? (faults/checkers {:faults [:kill], :kill-signal :kill})))
  (is (empty? (faults/checkers {:faults [:kill]})))
  (is (empty? (faults/checkers {:faults [:pause], :kill-signal :term}))))

(deftest kill-durations
  (let [usual #(take 20 (schedule/gaps (schedule/gaps-stream (schedule/streams 9) :db) 15))]
    (testing "without --kill-duration, the usual gaps and ops"
      (is (= (usual) (take 20 (process/gaps (schedule/streams 9) {:nemesis-interval 15}))))
      (is (= :kill (:f (first (kill-ops 1 {}))))))
    (testing "with it, the ops start with a start that starts nothing, then alternate"
      (let [ops (kill-ops 40 {:kill-duration [60 90], :kill-targets [:middle]})]
        (is (= process/first-start (first ops)))
        (is (= (take 39 (kill-ops 39 {:kill-targets [:middle]})) (rest ops)))
        (is (every? #(= :kill (:f %)) (take-nth 2 (rest ops))))
        (is (every? #(= :start (:f %)) (take-nth 2 ops)))))
    (testing "and the gaps alternate: the usual one after a start, a drawn one after a kill"
      (let [g (take 20 (process/gaps (schedule/streams 9) {:nemesis-interval 15
                                                          :kill-duration [60 90]}))]
        (is (= (take 10 (usual)) (take-nth 2 g)))
        (is (every? #(and (<= 60e9 % 90e9) (zero? (mod % 1000000000))) (take-nth 2 (rest g))))
        (is (< 3 (count (set (take-nth 2 (rest g))))))))
    (testing "under the simulator, a kill lasts what was drawn and the first comes a gap in"
      (let [hist  (->> (gt/simulate (gen/context {:concurrency 1})
                                    (gen/nemesis (gen/limit 21 (faults/generator
                                                                 (schedule/streams 9)
                                                                 {:nodes nodes, :faults [:kill]
                                                                  :kill-targets [:middle]
                                                                  :nemesis-interval 15
                                                                  :kill-duration [60 90]})))
                                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 1000000))))
                       (filter #(= :info (:type %))))
            spans (->> (rest hist) (partition 2)
                       (map (fn [[a b]] [(:f a) (:value a) (:f b) (- (:time b) (:time a))])))]
        (is (= :start (:f (first hist))))
        (is (= (first (usual)) (:time (second hist))))
        (is (= 10 (count spans)))
        (is (every? (fn [[a v b dt]] (and (= :kill a) (= ["n2"] v) (= :start b) (<= 60e9 dt 90e9)))
                    spans)))))
  (testing "only with kills, and not with pauses"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"--kill-duration needs"
                          (kill-ops 1 {:faults [:kill :pause], :kill-duration [60 90]})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"--kill-duration needs"
                          (doall (take 1 ((:ops process/fault) (schedule/stream (schedule/streams 11) :db)
                                                               {:nodes nodes, :faults [:pause]
                                                                :kill-duration [60 90]})))))))

(deftest options
  (let [parse #(cli/parse-opts % fiber/cli-opts)]
    (is (= [60 90] (:kill-duration (:options (parse ["--kill-duration" "60-90"])))))
    (is (= [75 75] (:kill-duration (:options (parse ["--kill-duration" "75"])))))
    (is (seq (:errors (parse ["--kill-duration" "90-60"]))))
    (is (nil? (:kill-duration (:options (parse [])))))
    (is (= :kill (:kill-signal (:options (parse [])))))
    (is (= 30 (:term-grace (:options (parse [])))))
    (is (= :term (:kill-signal (:options (parse ["--kill-signal" "term"])))))
    (is (= [:middle] (:kill-targets (:options (parse ["--kill-targets" "middle"])))))
    (is (seq (:errors (parse ["--kill-signal" "hup"]))))
    (is (seq (:errors (parse ["--term-grace" "0"]))))))
