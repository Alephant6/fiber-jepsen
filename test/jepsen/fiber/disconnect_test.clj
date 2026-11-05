(ns jepsen.fiber.disconnect-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.nemesis :as n]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [rpc :as rpc]
                          [schedule :as schedule]]
            [jepsen.fiber.faults.disconnect :as disconnect]))

(def nodes ["n1" "n2" "n3"])

(defn- ops
  [seed opts]
  (take 200 (disconnect/ops (schedule/stream (schedule/streams seed) :disconnect)
                            (merge {:nodes nodes} opts))))

(deftest flash-ops
  (testing "without the option, a disconnect stays until the next op's reconnect"
    (is (= (take 200 (cycle [:disconnect :reconnect])) (map :f (ops 4 {})))))
  (testing "with it, each fault is one flash of a channel's peers, for a drawn time"
    (let [fs (ops 4 {:disconnect-flash [100 5000]})]
      (is (every? #(= :flash-disconnect (:f %)) fs))
      (is (= #{["n1" "n2"] ["n2" "n3"]} (set (map (comp :edge :value) fs))))
      (is (every? #(<= 100 (:down-ms (:value %)) 5000) fs))
      (is (< 1000 (apply max (map (comp :down-ms :value) fs))))
      (is (> 1000 (apply min (map (comp :down-ms :value) fs))))
      (is (= fs (ops 4 {:disconnect-flash [100 5000]})))))
  (testing "a fixed time"
    (is (every? #(= 300 (:down-ms (:value %))) (ops 4 {:disconnect-flash [300 300]})))))

(deftest flash-nemesis
  (let [calls (atom [])
        nem   (disconnect/disconnect-nemesis)
        test  {:nodes nodes, :pubkeys (atom {"n1" "pk1", "n2" "pk2", "n3" "pk3"})}]
    (with-redefs [rpc/call (fn [node method & [params]]
                             (swap! calls conj [node method params (System/nanoTime)])
                             (case method
                               "node_info"  {:addresses ["/ip4/10.77.0.13/tcp/8228/p2p/Qm3"]}
                               "list_peers" {:peers [{:pubkey "pk1"}]}
                               nil))]
      (let [done (n/invoke! nem test {:type :info, :f :flash-disconnect
                                      :value {:edge ["n2" "n3"], :down-ms 300}})
            [[_ m0] [_ m1 p1 t1] [_ m2] [_ m3 p3 t3]] @calls]
        (is (= ["node_info" "disconnect_peer" "list_peers" "connect_peer"] [m0 m1 m2 m3]))
        (is (= {:pubkey "pk3"} p1))
        (is (= {:address "/ip4/10.77.0.13/tcp/8228/p2p/Qm3"} p3))
        (is (<= 300 (/ (- t3 t1) 1e6) 1000))
        (is (= {:from "n2", :to "n3", :down-ms 300, :disconnect :ok
                :peer-before-reconnect false, :reconnect :ok}
               (:value done)))))))

(deftest option
  (let [parse #(cli/parse-opts % fiber/cli-opts)]
    (is (nil? (:disconnect-flash (:options (parse [])))))
    (is (= [100 5000] (:disconnect-flash (:options (parse ["--disconnect-flash" "100-5000"])))))
    (is (seq (:errors (parse ["--disconnect-flash" "5000-100"]))))))

(deftest reconnect-to-a-node-that-is-down
  (testing "a node whose node_info fails (down or paused) is recorded, not thrown"
    (let [nem  (disconnect/disconnect-nemesis)
          test {:nodes ["n1" "n2" "n3" "n4"], :topology :ring
                :pubkeys (atom {"n1" "pk1", "n2" "pk2", "n3" "pk3", "n4" "pk4"})}]
      (with-redefs [rpc/call (fn [node method & [_params]]
                               (case method
                                 "node_info" (if (= "n4" node)
                                               (throw (java.net.ConnectException. "Connection refused"))
                                               {:addresses [(str "/ip4/10.77.0.1" (subs node 1) "/tcp/8228/p2p/Qm")]})
                                 nil))]
        (let [v (:value (n/invoke! nem test {:type :info, :f :reconnect}))]
          (is (= :ok (v "n1->n2")))
          (is (re-find #"^failed: node_info of n4 has no p2p address" (v "n3->n4"))))
        (let [v (:value (n/invoke! nem test {:type :info, :f :flash-disconnect
                                              :value {:edge ["n3" "n4"], :down-ms 1}}))]
          (is (re-find #"^failed: node_info of n4" (:reconnect v))))))))

(deftest reconnect-in-parallel
  (testing "a node that doesn't answer holds up the reconnect once, not once per pair"
    (let [nem  (disconnect/disconnect-nemesis)
          test {:nodes ["n1" "n2" "n3" "n4" "n5" "n6"], :topology :mesh
                :pubkeys (atom (into {} (for [i (range 1 7)] [(str "n" i) (str "pk" i)])))}]
      (with-redefs [rpc/call (fn [node method & [_params]]
                               (case method
                                 "node_info" (do (Thread/sleep 300)
                                                 {:addresses [(str "/ip4/10.77.0.1" (subs node 1) "/tcp/8228/p2p/Qm")]})
                                 nil))]
        (let [t0 (System/nanoTime)
              v  (:value (n/invoke! nem test {:type :info, :f :reconnect}))
              ms (/ (- (System/nanoTime) t0) 1e6)]
          (is (= 9 (count v)))
          (is (every? #{:ok} (vals v)))
          ; In a row, 9 pairs would take 2.7 s.
          (is (< ms 1500)))))))
