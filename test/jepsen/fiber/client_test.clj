(ns jepsen.fiber.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen [checker :as checker]
                    [client :as jc]
                    [history :as h]]
            [jepsen.fiber [checker :as fiber-checker]
                          [client :as client]]))

(def ^:private invoke
  {:type :invoke, :f :close, :process 0})

(defn- close-op
  "The :close completion when close! returns `requests`."
  [requests]
  (with-redefs [client/close! (fn [_test] requests)]
    (jc/invoke! (client/client) {} invoke)))

(deftest close-op-type
  (let [ok      {:channel "a", :by "n1", :result :ok, :tries 1}
        refused {:channel "b", :by "n2", :result "Channel not ready", :tries 20}]
    (testing "every request accepted: :ok, so the stats check sees a :close succeed"
      (let [op (close-op [ok (assoc ok :channel "c")])]
        (is (= :ok (:type op)))
        (is (= 2 (count (:value op))))
        (is (true? (:valid? (checker/check (checker/stats) {} (h/history [invoke op]) {}))))))
    (testing "an end kept refusing: :info, since the request may still have started a close"
      (is (= :info (:type (close-op [ok refused])))))
    (testing "the close check reports the request either way"
      (let [ch    {:channel_id "a", :state {:state_name "Closed"}}
            test  {:initial-channels (atom {"n1" [ch]})}
            read  {:type :ok, :f :read, :value {:channels {"n1" [ch], "n2" [ch]}}}]
        (doseq [op [(close-op [ok]) (close-op [ok refused])]]
          (let [r (checker/check (fiber-checker/close-checker) test [op read] {})]
            (is (true? (:valid? r)))
            (is (= (:value op) (:close-requests r)))))))))
