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
      (if (= :extra (:priority s))
        (is (re-matches #"[a-z]+(-[a-z]+)*" name))
        (do (is (re-matches #"c-\d\d[ab]?" name))
            (is (#{:p0 :p1 :p2} (:priority s)))))
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

(deftest c-16
  (let [{:keys [options errors]} (parse ["--scenario" "c-16"])]
    (is (empty? errors))
    (testing "nine faults that can run together, on the six-node mesh"
      (is (= #{:kill :pause :partition :disconnect :packet :chain-partition :cpu :memory :clock}
             (set (:faults options))))
      (is (= :mesh (:topology options)))
      (is (= 6 (count (:nodes options)))))
    (testing "kills, pauses, partitions and clock steps under fnn's 30 s deadline; closes fail the run"
      (is (< (second (:process-duration options)) 30))
      (is (< (second (:partition-duration options)) 30))
      (is (< (second (:clock-jump options)) 30))
      (is (not (:force-close-ok options)))
      (is (nil? (:force-close-ok-after options))))
    (testing "the narrower rule can be added"
      (is (= 30 (:force-close-ok-after (:options (parse ["--scenario" "c-16"
                                                         "--force-close-ok-after" "30"]))))))))

(deftest expand
  (testing "a scenario's arguments follow the option, so later ones override them"
    (let [{:keys [options errors]} (parse ["--scenario" "c-08" "--time-limit" "60"])]
      (is (empty? errors))
      (is (= [:pause] (:faults options)))
      (is (= 30 (:nemesis-interval options)))
      (is (= 60 (:time-limit options))))
    (is (= ["--rate" "9" "--scenario=c-08" "--faults" "pause" "--nemesis-interval" "30"
            "--nemesis-min-gap" "30" "--time-limit" "300" "--ledger" "--force-close-ok-after" "30"]
           (scenarios/expand ["--rate" "9" "--scenario=c-08"]))))
  (testing "other arguments pass through"
    (is (= ["test" "--faults" "kill"] (scenarios/expand ["test" "--faults" "kill"]))))
  (testing "an unknown name is an error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown scenario"
                          (scenarios/expand ["--scenario" "c-99"])))
    (is (thrown? clojure.lang.ExceptionInfo (scenarios/expand ["--scenario"])))))

(deftest power-scenarios
  (doseq [[name sync] [["power-sync-on" true] ["power-sync-off" false]]]
    (let [{:keys [options errors]} (parse ["--scenario" name])]
      (is (empty? errors))
      (is (= [:power] (:faults options)))
      (is (= sync (:store-sync-writes options))))))

(deftest force-close-rule
  (let [opts (into {} (for [[name _] scenarios/scenarios]
                        [name (:options (parse ["--scenario" name]))]))]
    (testing "the scenarios whose faults can outlast fnn's 30 s peer deadline excuse the closes it explains"
      (is (= #{"c-01a" "c-01b" "c-02a" "c-02b" "c-04" "c-06" "c-08" "power-sync-off" "power-sync-on"}
             (set (for [[name o] opts :when (= 30 (:force-close-ok-after o))] name)))))
    (testing "those with short kills, pauses or power losses space them, so that two in a row can't add up to an outage over the deadline; c-06's partitions each outlast it"
      (is (= #{"c-01a" "c-01b" "c-02a" "c-02b" "c-04" "c-08" "power-sync-off" "power-sync-on"}
             (set (for [[name o] opts
                        :when (and (= 30 (:force-close-ok-after o)) (= 30 (:nemesis-min-gap o)))]
                    name))))
      (is (zero? (:nemesis-min-gap (opts "c-06")))))
    (testing "the scenarios whose point is a close excuse every one"
      (is (= #{"c-03" "c-11"} (set (for [[name o] opts :when (:force-close-ok o)] name)))))
    (testing "every kind of outage counts unless a scenario says otherwise"
      (is (every? (comp nil? :force-close-ok-kinds) (vals opts)))
      (is (= [:pause :partition]
             (:force-close-ok-kinds (:options (parse ["--scenario" "c-06" "--force-close-ok-kinds"
                                                      "pause,partition"])))))
      (is (seq (:errors (parse ["--force-close-ok-kinds" "pause,clock"])))))))

(deftest ledger-coverage
  (testing "every scenario checks the ledger, except where on-chain closes or swaps move the balances"
    (is (= #{"c-03" "c-09" "c-10" "c-11" "c-15"}
           (set (for [[name s] scenarios/scenarios
                      :when (not (:ledger (:options (parse ["--scenario" name]))))]
                  name))))))

(deftest matrix
  (testing "all, a priority, or names"
    (is (= (sort (keys scenarios/scenarios)) (scenarios/select "all") (scenarios/select nil)))
    (is (every? #(= :p0 (:priority (scenarios/scenarios %))) (scenarios/select "p0")))
    (is (= ["c-01b" "c-04" "c-08"] (scenarios/select "c-08, c-04,c-01b,c-04")))
    (is (= (scenarios/select "p1") (scenarios/select "p1,c-08")))
    (is (thrown? clojure.lang.ExceptionInfo (scenarios/select "c-99"))))
  (testing "extra: the scenarios the plan doesn't have"
    (is (= ["power-sync-off" "power-sync-on"] (scenarios/select "extra"))))
  (testing "run.sh's arguments: a scenario's compose file, then the scenario"
    (is (= {:name "c-15", :priority "p1", :args "--compose cch --scenario c-15"}
           (first (scenarios/matrix "c-15"))))
    (is (= {:name "power-sync-on", :priority "extra", :args "--compose lazyfs --scenario power-sync-on"}
           (first (scenarios/matrix "power-sync-on"))))
    (is (= "--scenario c-04" (:args (first (scenarios/matrix "c-04")))))))
