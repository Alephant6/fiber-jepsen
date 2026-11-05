(ns jepsen.fiber.expiry-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [expiry :as expiry]
                          [onchain-test :as ot]
                          [rpc :as rpc]
                          [topology :as topology]]))

(def hash-a "0xaa")
(def hash-b "0xbb")

(defn tlc
  [hash amount & {:keys [inbound? forwarding]}]
  {:id                    "0x0"
   :amount                (rpc/int->hex amount)
   :expiry                (rpc/int->hex 1000)
   :payment_hash          hash
   :forwarding_channel_id forwarding
   :status                (if inbound? {:Inbound "Committed"} {:Outbound "Committed"})})

(defn settle
  "n1 opened the channel and force-closed it holding `tlcs`, with `trace`."
  [tlcs trace]
  {:channels {"n1" [(assoc (ot/record "0xc1" 700 100 :tlcs tlcs) :state {:state_name "Closed"})]
              "n2" [(assoc (ot/record "0xc1" 100 700 :acceptor? true) :state {:state_name "Closed"})]}
   :traces   {"0xc1" trace}
   :locks    ot/locks})

; n1 force-closes holding a TLC of 60 and gets it back: 700 + a reserve of 100,
; less its settlement fee of 3. The channel's fees add up to 10.
(def refunded ot/forced)

; The same, but n2 claimed the TLC: n1 gets 60 less.
(def claimed
  (ot/channel-trace [(ot/tx "0xcommit" 20 [["F" 1000]] [["C1" 996]])
                     (ot/tx "0xsettle1" 31 [["C1" 996] ["L1" 50]] [["C2" 256] ["L1" 787]])
                     (ot/tx "0xsettle2" 33 [["C2" 256] ["L2" 50]] [["L2" 303]])]))

(defn final
  [& statuses]
  (vec (for [[hash status] (partition 2 statuses)] {:node "n1", :hash hash, :status status})))

(deftest refunds
  (testing "a Failed payment's TLC came back"
    (let [r (expiry/analyze (settle [(tlc hash-a 60)] refunded) (final hash-a "Failed"))]
      (is (true? (:valid? r)))
      (is (= 1 (:payer-tlcs r)))
      (is (= {"Failed" 1} (:by-payment r)))
      (is (= 60 (:refunded r)))
      (is (= {:received 797, :least 790, :refunds 60, :short 0, :ok? true}
             (select-keys (first (:checks r)) [:received :least :refunds :short :ok?])))))
  (testing "a Failed payment's TLC went to the peer: the payer lost it"
    (let [r (expiry/analyze (settle [(tlc hash-a 60)] claimed) (final hash-a "Failed"))]
      (is (false? (:valid? r)))
      (is (= [{:channel "0xc1", :node "n1", :received 737, :least 790, :short 53}]
             (map #(select-keys % [:channel :node :received :least :short]) (:lost r))))))
  (testing "a Success payment's TLC may go to the peer"
    (is (true? (:valid? (expiry/analyze (settle [(tlc hash-a 60)] claimed) (final hash-a "Success"))))))
  (testing "a payment still in flight after settling fails the run"
    (let [r (expiry/analyze (settle [(tlc hash-a 60)] refunded) (final hash-a "Inflight"))]
      (is (false? (:valid? r)))
      (is (= [{:node "n1", :hash hash-a, :status "Inflight"}] (:unresolved r)))))
  (testing "only TLCs the node offered for its own payments count"
    (let [tlcs [(tlc hash-b 60 :forwarding "0xc2") (tlc hash-b 30 :inbound? true)]
          r    (expiry/analyze (settle tlcs refunded) (final hash-a "Success"))]
      (is (= 0 (:payer-tlcs r)))
      (is (= :unknown (:valid? r)))))
  (testing "a channel whose cells aren't all spent can't be checked yet"
    (let [r (expiry/analyze (settle [(tlc hash-a 60)] (assoc refunded :live [{:tx "0xcommit", :index 0}]))
                            (final hash-a "Failed"))]
      (is (= :unknown (:valid? r)))
      (is (= 1 (count (:unsettled r))))))
  (testing "missing reads"
    (is (= :unknown (:valid? (expiry/analyze nil (final hash-a "Failed")))))
    (is (= :unknown (:valid? (expiry/analyze (settle [] refunded) nil))))))

(deftest offered
  (is (expiry/offered? {:status {:Outbound "RemoteRemoved"}}))
  (is (not (expiry/offered? {:status {:Inbound "Committed"}})))
  (is (expiry/offered? {:status "Outbound(Committed)"})))

(deftest sets-the-delta-and-waits-for-the-graph
  (let [calls  (atom [])
        seen   (atom 0)
        delta  20000
        update (fn [d] {:tlc_expiry_delta (rpc/int->hex d)})
        graph  (fn [] {:channels [{:update_info_of_node1 (update (if (< 1 @seen) delta 14400000))
                                   :update_info_of_node2 (update delta)}]})]
    (with-redefs [rpc/call (fn [node method params & _]
                             (swap! calls conj [node method params])
                             (case method
                               "list_channels"  {:channels [{:channel_id (str "0xc-" node)}]}
                               "update_channel" nil
                               "graph_channels" (do (swap! seen inc) (graph))))]
      (topology/set-tlc-expiry-delta! {:nodes ["n1" "n2"]} delta))
    (is (= [["n1" "update_channel" {:channel_id "0xc-n1", :tlc_expiry_delta "0x4e20"}]
            ["n2" "update_channel" {:channel_id "0xc-n2", :tlc_expiry_delta "0x4e20"}]]
           (filter #(= "update_channel" (second %)) @calls)))
    (testing "it read the graph until the delta showed"
      (is (< 2 @seen)))))

(deftest options
  (let [parse #(cli/parse-opts % fiber/cli-opts)]
    (is (true? (:expiry-check (:options (parse ["--expiry-check"]))))))
  (is (= :unknown (:valid? (expiry/analyze nil nil)))))
