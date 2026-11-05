(ns jepsen.fiber.channels-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [channels :as channels]
                          [checker :as fc]
                          [schedule :as schedule]]))

(def nodes ["n1" "n2" "n3"])

(def opts
  {:nodes nodes, :rate 5, :channel-ops [:open :close :force-close], :channel-interval 2})

(defn client-ops
  "The client ops of the fault window for `opts`, under Jepsen's simulator."
  [seed opts]
  (->> (gt/simulate (gen/context {:concurrency 3})
                    (gen/clients (gen/limit 300 (fiber/client-ops opts (schedule/streams seed))))
                    (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 2000000))))
       (filter #(= :invoke (:type %)))
       (map (juxt :f :value))))

(deftest ops
  (let [draw #(take 200 (channels/ops (schedule/stream (schedule/streams %1) :channels) %2))]
    (testing "the seed fixes them"
      (is (= (draw 7 opts) (draw 7 opts)))
      (is (not= (draw 7 opts) (draw 8 opts))))
    (testing "only the kinds asked for, on the topology's edges"
      (is (= #{:open-channel :close-channel :force-close} (set (map :f (draw 7 opts)))))
      (is (= #{:force-close} (set (map :f (draw 7 (assoc opts :channel-ops [:force-close]))))))
      (is (= #{0 1} (set (map (comp :edge :value) (draw 7 opts)))))
      (is (every? (comp :pick :value) (remove #(= :open-channel (:f %)) (draw 7 opts)))))
    (testing "none without --channel-ops"
      (is (nil? (channels/generator (schedule/streams 7) (assoc opts :channel-ops [])))))))

(deftest payments-unchanged
  (testing "adding the channel workload changes no payment"
    (doseq [seed [1 2]]
      (let [pays  #(filter (comp #{:pay} first) %)
            alone (pays (client-ops seed (assoc opts :channel-ops [])))
            mixed (pays (client-ops seed opts))
            n     (min (count alone) (count mixed))]
        (is (< 50 n))
        (is (= (take n alone) (take n mixed)))
        (is (some (comp #{:open-channel :close-channel :force-close} first)
                  (client-ops seed opts)))))))

(defn ch
  [id state & {:keys [flags tlcs]}]
  {:channel_id id, :state {:state_name state, :state_flags flags}, :pending_tlcs (or tlcs [])})

(deftest final-states
  (let [read (fn [m] {:channels m})]
    (testing "ready or closed, on both ends alike"
      (is (true? (:valid? (channels/analyze
                            (read {"n1" [(ch "a" "ChannelReady") (ch "b" "Closed" :flags "COOPERATIVE")]
                                   "n2" [(ch "a" "ChannelReady") (ch "b" "Closed" :flags "UNCOOPERATIVE_REMOTE")]})
                            [])))))
    (testing "an end stuck in between"
      (let [r (channels/analyze (read {"n1" [(ch "a" "AwaitingChannelReady")]
                                       "n2" [(ch "a" "ChannelReady")]})
                                [])]
        (is (false? (:valid? r)))
        (is (= [{:channel "a", :node "n1", :state {:state_name "AwaitingChannelReady", :state_flags nil}}]
               (:between-states r)))))
    (testing "ends that disagree"
      (let [r (channels/analyze (read {"n1" [(ch "a" "Closed" :flags "UNCOOPERATIVE_LOCAL")]
                                       "n2" [(ch "a" "ChannelReady")]})
                                [])]
        (is (false? (:valid? r)))
        (is (= ["a"] (map :channel (:ends-disagree r))))))
    (testing "operation counts"
      (is (= {"force-close ok" 1, "open-channel fail" 1}
             (:ops (channels/analyze (read {})
                                     [{:type :fail, :f :open-channel}
                                      {:type :ok, :f :force-close, :value {:channel "b"}}
                                      {:type :invoke, :f :force-close}])))))))

(deftest channel-checker
  (let [history [{:type :ok, :f :force-close, :value {:channel "b"}}
                 {:type :info, :f :close-channel, :value {:channel "c"}}
                 {:type :fail, :f :close-channel, :value {:channel "d"}, :error :keeping-last}]]
    (is (= #{"b" "c"} (fc/closed-on-purpose history)))
    (testing "TLCs pending on a channel closed on purpose settle on chain"
      (let [final {"n1" [(ch "b" "Closed" :tlcs [{:payment_hash "h1"}])
                         (ch "a" "ChannelReady" :tlcs [{:payment_hash "h2"}])]}]
        (is (= #{"h1"} (fc/held-on-chain final #{"b"})))))
    (testing "a setup channel closed on purpose is not a problem"
      (let [init  {"n1" [{:channel_id "b", :local_balance "0x10", :remote_balance "0x0"}]}
            final {"n1" [{:channel_id "b", :local_balance "0x10", :remote_balance "0x0"
                          :state {:state_name "Closed"}}]}]
        (is (= [:not-ready "Closed"] (first (:problems (first (fc/channel-problems init final))))))
        (is (empty? (fc/channel-problems init final #{"b"})))))))
