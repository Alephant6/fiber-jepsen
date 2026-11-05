(ns jepsen.fiber.channels-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [channels :as channels]
                          [checker :as fc]
                          [rpc :as rpc]
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
    (testing "the wait for the ends to agree is reported"
      (let [settle {:waited-s 212, :settled? true
                    :at-start {:between [], :disagree [{:channel "a"}]}}
            r      (channels/analyze (read {"n1" [(ch "a" "Closed" :flags "UNCOOPERATIVE_LOCAL")]
                                            "n2" [(ch "a" "Closed" :flags "UNCOOPERATIVE_REMOTE")]})
                                     [{:type :invoke, :f :settle-channels}
                                      {:type :ok, :f :settle-channels, :value settle}])]
        (is (true? (:valid? r)))
        (is (= settle (:settle-wait r)))))
    (testing "operation counts"
      (is (= {"force-close ok" 1, "force-close skipped" 1, "open-channel fail" 1}
             (:ops (channels/analyze (read {})
                                     [{:type :fail, :f :open-channel}
                                      {:type :ok, :f :force-close, :value {:channel "b"}}
                                      {:type :ok, :f :force-close, :value {:skipped :keeping-last}}
                                      {:type :invoke, :f :force-close}])))))))

(deftest channel-checker
  (let [history [{:type :ok, :f :force-close, :value {:channel "b"}}
                 {:type :info, :f :close-channel, :value {:channel "c"}}
                 {:type :fail, :f :close-channel, :value {:channel "d"}, :error :node-down}
                 {:type :ok, :f :close-channel, :value {:edge 0, :skipped :keeping-last}}]]
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

(deftest settled-tlcs
  (let [tlc (fn [hash status] {:payment_hash hash, :status status})]
    (testing "a TLC removed on this end is settled, in either direction and either format"
      (is (fc/settled-tlc? (tlc "h" {:Outbound "RemoveAckConfirmed"})))
      (is (fc/settled-tlc? (tlc "h" {:Inbound "RemoveAckConfirmed"})))
      (is (fc/settled-tlc? (tlc "h" "Inbound(RemoveAckConfirmed)")))
      (is (not-any? fc/settled-tlc? [(tlc "h" {:Outbound "RemoveWaitAck"})
                                     (tlc "h" {:Inbound "LocalRemoved"})
                                     (tlc "h" {:Outbound "Committed"})
                                     (tlc "h" "Outbound(Committed)")
                                     (tlc "h" nil)])))
    (let [done    (tlc "h1" {:Outbound "RemoveAckConfirmed"})
          done-in (tlc "h1" {:Inbound "RemoveAckConfirmed"})
          init    {"n1" [{:channel_id "a", :local_balance "0x10", :remote_balance "0x10"}]
                   "n2" [{:channel_id "a", :local_balance "0x10", :remote_balance "0x10"}]}
          final   (fn [t1 t2]
                    {"n1" [(assoc (ch "a" "ChannelReady" :tlcs t1) :local_balance "0x10" :remote_balance "0x10")]
                     "n2" [(assoc (ch "a" "ChannelReady" :tlcs t2) :local_balance "0x10" :remote_balance "0x10")]})]
      (testing "a removal both ends confirmed leaves the channel clean, though both still list the TLC"
        (is (empty? (fc/channel-problems init (final [done] [done-in]))))
        (is (= {:ready-clean 2} (fc/channel-summary (final [done] [done-in]))))
        (is (empty? (fc/held-on-chain (final [done] [done-in]) #{"a"}))))
      (testing "a removal the offering end hasn't confirmed is still pending there"
        (let [stuck (tlc "h1" {:Outbound "RemoveWaitAck"})]
          (is (= [{:channel "a", :node "n1", :peer "n2", :problems [[:pending-tlcs 1]]}]
                 (fc/channel-problems init (final [stuck] [done-in]))))
          (is (= {:ready-clean 1, :ready-with-pending-tlcs 1}
                 (fc/channel-summary (final [stuck] [done-in]))))))
      (testing "a settled TLC beside a live one counts only the live one"
        (is (= [[:pending-tlcs 1]]
               (:problems (first (fc/channel-problems
                                   init (final [done (tlc "h2" {:Outbound "Committed"})] [done-in]))))))))))

(deftest unsettled
  (is (= {:between [], :disagree []}
         (channels/unsettled {"n1" [(ch "a" "ChannelReady") (ch "b" "Closed" :flags "COOPERATIVE")]
                              "n2" [(ch "a" "ChannelReady")]})))
  (let [u (channels/unsettled {"n1" [(ch "a" "Closed" :flags "UNCOOPERATIVE_LOCAL")
                                     (ch "b" "ShuttingDown" :flags "WAITING_COMMITMENT_CONFIRMATION")]
                               "n2" [(ch "a" "ChannelReady")
                                     (ch "b" "Closed" :flags "UNCOOPERATIVE_LOCAL")]})]
    (is (= ["a"] (map :channel (:disagree u))))
    (is (= [["b" "n1"]] (map (juxt :channel :node) (:between u))))))

(deftest held-upstream
  (let [final {"n1" [(ch "up" "ChannelReady" :tlcs [{:payment_hash "h1"}])]
               "n2" [(ch "up" "ChannelReady" :tlcs [{:payment_hash "h1"}])
                     (ch "down" "Closed" :tlcs [{:payment_hash "h1"}])
                     (ch "other" "ChannelReady" :tlcs [{:payment_hash "h1"} {:payment_hash "h2"}])]}
        probs [{:channel "up", :node "n1", :problems [[:pending-tlcs 1]]}
               {:channel "up", :node "n2", :problems [[:pending-tlcs 1] [:mirror {}]]}
               {:channel "other", :node "n2", :problems [[:pending-tlcs 2]]}]]
    (testing "TLCs whose payments are held on a closed channel are excused, others aren't"
      (is (= [{:channel "up", :node "n2", :problems [[:mirror {}]]}
              {:channel "other", :node "n2", :problems [[:pending-tlcs 2]]}]
             (fc/excuse-held probs final (fc/held-on-chain final #{"down"})))))
    (testing "nothing closed on purpose: nothing excused"
      (is (= probs (fc/excuse-held probs final #{}))))))

(deftest sender-balance
  (let [final {"n1" [(assoc (ch "a" "Closed") :local_balance "0x64")
                     (assoc (ch "b" "ChannelReady") :local_balance "0xa")
                     (assoc (ch "c" "ChannelReady") :local_balance "0x14")]
               "n2" [(ch "a" "Closed") (ch "b" "ChannelReady") (ch "c" "ChannelReady")]}]
    (is (= 20 (fc/sender-balance final "n1" "n2")))
    (is (nil? (fc/sender-balance {"n1" [(assoc (ch "a" "Closed") :local_balance "0x64")]
                                  "n2" [(ch "a" "Closed")]}
                                 "n1" "n2")))))

(defn- fake-nodes
  "rpc/call over fake nodes: list_channels answers from `views` ({node {channel
  state}}, all with the one peer), shutdown_channel is recorded in `closed`,
  and a node missing from `views` is down."
  [views closed]
  (fn [node method params & _]
    (case method
      "list_channels"
      (if-let [view (get views node)]
        {:channels (for [[id st] view] {:channel_id id, :state {:state_name st}})}
        (throw (java.net.ConnectException. "down")))
      "shutdown_channel" (do (swap! closed conj [node (:channel_id params)]) {}))))

(deftest close-counts-channels-ready-at-both-ends
  (let [test {:nodes nodes, :pubkeys (atom {"n1" "p1", "n2" "p2", "n3" "p3"}), :channel-keep 1}
        op   {:type :invoke, :f :force-close, :value {:edge 0, :flip? false, :pick 0}}]
    (testing "n2 force-closed 0xa, and n1 hasn't noticed: only 0xb is ready, so it is kept"
      (let [closed (atom [])]
        (with-redefs [rpc/call (fake-nodes {"n1" {"0xa" "ChannelReady", "0xb" "ChannelReady"}
                                            "n2" {"0xa" "Closed", "0xb" "ChannelReady"}}
                                           closed)]
          (let [res (channels/close! test op true)]
            (is (= :ok (:type res)))
            (is (= :keeping-last (get-in res [:value :skipped])))
            (is (= 1 (get-in res [:value :ready])))
            (is (empty? @closed))))))
    (testing "two channels ready at both ends: one is closed"
      (let [closed (atom [])]
        (with-redefs [rpc/call (fake-nodes {"n1" {"0xa" "ChannelReady", "0xb" "ChannelReady"}
                                            "n2" {"0xa" "ChannelReady", "0xb" "ChannelReady"}}
                                           closed)]
          (let [res (channels/close! test op true)]
            (is (= :ok (:type res)))
            (is (= "0xa" (get-in res [:value :channel])))
            (is (= [["n1" "0xa"]] @closed))))))
    (testing "the peer can't be read: the acting end's view alone"
      (let [closed (atom [])]
        (with-redefs [rpc/call (fake-nodes {"n1" {"0xa" "ChannelReady", "0xb" "ChannelReady"}}
                                           closed)]
          (let [res (channels/close! test op false)]
            (is (= :ok (:type res)))
            (is (= :unavailable (get-in res [:value :peer-view])))
            (is (= 1 (count @closed)))))))))
