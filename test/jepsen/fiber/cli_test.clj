(ns jepsen.fiber.cli-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.fiber :as fiber]
            [jepsen.nemesis.combined :as nc]))

(defn faults
  [& args]
  (let [{:keys [options errors]} (cli/parse-opts args fiber/cli-opts)]
    (is (empty? errors))
    (:faults options)))

(deftest faults-option
  (is (= [:kill] (faults)))
  (is (= [:kill :partition] (faults "--faults" "kill,partition")))
  (testing "none runs without faults"
    (let [opts {:faults [], :nodes ["n1" "n2" "n3"], :rate 5, :time-limit 30, :quiesce 30
                :nemesis-interval 15, :kill-targets [:one :all]}]
      (is (= [] (faults "--faults" "none")))
      (is (= nc/noop (fiber/nemesis-package nil opts)))
      (is (= "fiber-none" (:name (fiber/fiber-test opts))))))
  (testing "an unknown fault is rejected"
    (is (seq (:errors (cli/parse-opts ["--faults" "kill,flood"] fiber/cli-opts))))))
