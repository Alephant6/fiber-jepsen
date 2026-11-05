(ns jepsen.fiber.clock-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [faults :as faults]
                          [rpc :as rpc]
                          [schedule :as schedule]]
            [jepsen.fiber.faults.clock :as clock]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :faults [:clock], :clock-targets [:one :all], :clock-jump [60 1800]})

(defn draw
  [seed opts n]
  (take n (clock/ops (schedule/stream (schedule/streams seed) :clock) opts)))

(deftest ops
  (let [ops   (draw 9 opts 400)
        jumps (map :value (filter (comp #{:clock-jump} :f) ops))]
    (testing "each jump is followed by a reset"
      (is (= (take 400 (cycle [:clock-jump :clock-reset])) (map :f ops))))
    (testing "the seed fixes the jumps"
      (is (= ops (draw 9 opts 400)))
      (is (not= ops (draw 10 opts 400))))
    (testing "forward and back, within the range, on one node or all"
      (is (some pos? (map :offset-s jumps)))
      (is (some neg? (map :offset-s jumps)))
      (is (every? #(<= 60 (abs (:offset-s %)) 1800) jumps))
      (is (= #{1 3} (set (map (comp count :nodes) jumps)))))))

(deftest offset-file
  (is (= "+300" (clock/spec 300)))
  (is (= "-60" (clock/spec -60)))
  (is (= "+0" (clock/spec 0)))
  (is (= [60 1800] (clock/parse-range "60-1800")))
  (is (= [300 300] (clock/parse-range "300"))))

(deftest environment
  (testing "fnn runs under libfaketime only with the clock fault"
    (is (= clock/library (:LD_PRELOAD (faults/fnn-env opts))))
    (is (= "1" (:FAKETIME_DONT_FAKE_MONOTONIC (faults/fnn-env opts))))
    (is (= {} (faults/fnn-env {:faults [:kill]})))
    (is (= {} (:fnn-env (fiber/fiber-test {:faults [:kill], :nodes nodes, :rate 5, :time-limit 30
                                            :quiesce 30, :nemesis-interval 15
                                            :kill-targets [:one]}))))))

(deftest cli
  (let [parse (fn [& args] (cli/parse-opts args fiber/cli-opts))]
    (is (= [60 1800] (:clock-jump (:options (parse)))))
    (is (= [5 5] (:clock-jump (:options (parse "--clock-jump" "5")))))
    (is (seq (:errors (parse "--clock-jump" "0-10"))))
    (is (seq (:errors (parse "--clock-targets" "some"))))
    (is (= [:clock] (:faults (:options (parse "--faults" "clock")))))))

(deftest invoice-timestamp
  (is (= 1762315200123 (clock/invoice-timestamp-ms
                         {:invoice {:data {:timestamp (rpc/int->hex 1762315200123)}}})))
  (testing "seconds are scaled"
    (is (= 1762315200000 (clock/invoice-timestamp-ms
                           {:invoice {:data {:timestamp (rpc/int->hex 1762315200)}}}))))
  (is (nil? (clock/invoice-timestamp-ms {}))))

(defn op
  [f set measured]
  {:type :info, :process :nemesis, :f f, :time 1
   :value {:offset-s set, :fnn-offset-s measured}})

(deftest check
  (testing "fnn's clock followed every change"
    (let [r (clock/check [{:type :info, :f :clock-jump, :value {:nodes ["n2"], :offset-s 300}}
                          (op :clock-jump 300 {"n2" 300.4})
                          (op :clock-reset 0 {"n1" 0.1, "n2" -0.2, "n3" 0.0})])]
      (is (true? (:valid? r)))
      (is (= [300] (:offsets-s r)))
      (is (= 4 (:measured r)))))
  (testing "a node whose clock didn't move"
    (let [r (clock/check [(op :clock-jump -600 {"n1" 0.3})])]
      (is (false? (:valid? r)))
      (is (= [{:time 1, :f :clock-jump, :node "n1", :set -600, :measured 0.3}] (:not-taken r)))))
  (testing "a node that didn't answer doesn't count, and nothing measured is unknown"
    (is (= :unknown (:valid? (clock/check [(op :clock-jump 60 {"n1" "failed: Connection refused"})]))))
    (is (= :unknown (:valid? (clock/check []))))))

(deftest registered
  (is (contains? (faults/checkers opts) :clock)))
