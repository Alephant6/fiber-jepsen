(ns jepsen.fiber.process-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.checker :as checker]
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

(deftest options
  (let [parse #(cli/parse-opts % fiber/cli-opts)]
    (is (= :kill (:kill-signal (:options (parse [])))))
    (is (= 30 (:term-grace (:options (parse [])))))
    (is (= :term (:kill-signal (:options (parse ["--kill-signal" "term"])))))
    (is (= [:middle] (:kill-targets (:options (parse ["--kill-targets" "middle"])))))
    (is (= [:one :middle] (:kill-targets (:options (parse ["--kill-targets" "one,middle"])))))
    (testing "a typo is an error, not a run whose every kill throws"
      (is (seq (:errors (parse ["--kill-targets" "midle"]))))
      (is (seq (:errors (parse ["--kill-targets" "one,al"])))))
    (is (seq (:errors (parse ["--kill-signal" "hup"]))))
    (is (seq (:errors (parse ["--term-grace" "0"]))))))

(deftest pause-effect
  (let [pause #(process/effect {:f :pause, :value %})]
    (is (nil? (pause {"n2" "T"})))
    (is (nil? (pause {"n1" "Tl", "n2" "Tsl"})))
    (testing "fnn was down: killed by another fault, or exited and not reaped"
      (is (= :skipped (pause {"n2" :not-running})))
      (is (= :skipped (pause {"n2" nil, "n3" "Z"}))))
    (testing "a running fnn the signal didn't stop"
      (is (= "fnn not stopped: n1 Ssl, n3 R" (pause {"n3" "R", "n2" "T", "n1" "Ssl"}))))
    (testing "only pause completions"
      (is (nil? (process/effect {:f :pause, :value ["n1"]})))
      (is (nil? (process/effect {:f :resume, :value {"n1" "Ssl"}})))
      (is (nil? (process/effect {:f :kill, :value {"n1" nil}}))))))

(deftest process-duration
  (let [opts   {:nodes nodes, :faults [:kill :pause], :kill-targets [:one], :nemesis-interval 45}
        ops    #(take 40 ((:ops process/fault) (schedule/stream (schedule/streams 11) :db) %))
        gaps   #(take 40 ((:gaps process/fault) (schedule/streams 11) %))
        usual  (gaps opts)]
    (testing "without the option: the ops and gaps of before"
      (is (= usual (take 40 (schedule/gaps (schedule/gaps-stream (schedule/streams 11) :db) 45))))
      (is (some #(= [:pause :kill] %) (map (comp vec (partial map :f)) (partition 2 1 (ops opts))))))
    (testing "with it, each kill or pause is followed by its own recovery after a drawn time"
      (let [o  (assoc opts :process-duration [5 15])
            fs (map :f (ops o))
            gs (gaps o)]
        (is (every? #{[:kill :start] [:pause :resume]} (map vec (partition 2 fs))))
        (is (= #{:kill :pause} (set (take-nth 2 fs))))
        (is (every? #(<= 5e9 % 15e9) (take-nth 2 gs)))
        (is (= (take 20 usual) (take-nth 2 (rest gs))))))
    (testing "the option parses"
      (is (= [5 15] (:process-duration (:options (cli/parse-opts ["--process-duration" "5-15"] fiber/cli-opts)))))
      (is (seq (:errors (cli/parse-opts ["--process-duration" "15-5"] fiber/cli-opts)))))))
