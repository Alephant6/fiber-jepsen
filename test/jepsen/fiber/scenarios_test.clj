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
      (is (contains? #{nil :debug} (:build s)))
      (let [{:keys [options errors]} (parse ["--scenario" name])]
        (is (empty? errors))
        (is (= name (:scenario options)))))))

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
