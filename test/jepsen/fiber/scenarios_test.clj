(ns jepsen.fiber.scenarios-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.cli :as jc]
            [jepsen.fiber :as fiber]
            [jepsen.fiber.scenarios :as scenarios]))

(defn parse
  "Parses `args` as `lein run test` does: Jepsen's options and this suite's."
  [args]
  (cli/parse-opts (scenarios/expand args) (jc/merge-opt-specs jc/test-opt-spec fiber/cli-opts)))

(deftest every-scenario-parses
  (doseq [[name s] scenarios/scenarios]
    (testing name
      (is (re-matches #"c-\d\d[ab]?" name))
      (is (#{:p0 :p1 :p2} (:priority s)))
      (is (every? string? (:args s)))
      (let [{:keys [options errors]} (parse ["--scenario" name])]
        (is (empty? errors))
        (is (= name (:scenario options)))))))

(deftest s1-scenarios
  (testing "A-B: a later --nodes overrides run.sh's"
    (doseq [name ["c-01a" "c-05"]]
      (is (= ["n1" "n2"] (:nodes (:options (parse ["--nodes" "n1,n2,n3" "--scenario" name])))))))
  (testing "the middle node with invoices, by SIGTERM and by SIGKILL"
    (let [a (:options (parse ["--scenario" "c-02a"]))
          b (:options (parse ["--scenario" "c-02b"]))]
      (is (= [:middle] (:kill-targets a) (:kill-targets b)))
      (is (= [:invoice] (:pay-kinds a) (:pay-kinds b)))
      (is (= [:term :kill] [(:kill-signal a) (:kill-signal b)]))))
  (testing "flashes of 100 ms to 5 s"
    (is (= [100 5000] (:disconnect-flash (:options (parse ["--scenario" "c-05"])))))))

(deftest expand
  (testing "a scenario's arguments follow the option, so later ones override them"
    (let [{:keys [options errors]} (parse ["--scenario" "c-08" "--time-limit" "60"])]
      (is (empty? errors))
      (is (= [:pause] (:faults options)))
      (is (= 30 (:nemesis-interval options)))
      (is (= 60 (:time-limit options))))
    (is (= ["--rate" "9" "--scenario=c-08" "--faults" "pause" "--nemesis-interval" "30"
            "--time-limit" "300"]
           (scenarios/expand ["--rate" "9" "--scenario=c-08"]))))
  (testing "other arguments pass through"
    (is (= ["test" "--faults" "kill"] (scenarios/expand ["test" "--faults" "kill"]))))
  (testing "an unknown name is an error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown scenario"
                          (scenarios/expand ["--scenario" "c-99"])))
    (is (thrown? clojure.lang.ExceptionInfo (scenarios/expand ["--scenario"])))))
