(ns jepsen.fiber.ledger-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.fiber [checker :as fc]
                          [client :as client]
                          [ledger :as ledger]
                          [rpc :as rpc]]))

(def nodes ["n1" "n2" "n3"])

(defn channel
  ([id local remote] (channel id local remote "ChannelReady" []))
  ([id local remote state tlcs]
   {:channel_id     id
    :state          {:state_name state}
    :local_balance  (rpc/int->hex local)
    :remote_balance (rpc/int->hex remote)
    :pending_tlcs   tlcs}))

(defn line-channels
  "Channel records for n1 - n2 - n3: `a` and `b` are n1's and n2's local balances on
  channel 0x12, `c` and `d` are n2's and n3's on channel 0x23."
  [a b c d]
  {"n1" [(channel "0x12" a b)]
   "n2" [(channel "0x12" b a) (channel "0x23" c d)]
   "n3" [(channel "0x23" d c)]})

(def initial (line-channels 1000 500 1000 500))

(defn pay
  "A completed :pay op as the client records it."
  ([type id from to amount] (pay type id from to amount nil))
  ([type id from to amount extra]
   {:type  type
    :f     :pay
    :value (merge {:id id, :amount amount, :from from, :to to} extra)}))

(defn listed
  [hash id status fee]
  {:hash hash, :id id, :payment-status status, :fee fee})

(defn check
  [final listing ops]
  (ledger/analyze nodes initial {:channels final, :listed listing} ops))

(deftest helpers
  (is (= ["n2"] (ledger/intermediates nodes "n1" "n3")))
  (is (= ["n2"] (ledger/intermediates nodes "n3" "n1")))
  (is (= [] (ledger/intermediates nodes "n1" "n2")))
  (is (nil? (ledger/intermediates nodes "n1" "n9")))
  (testing "the attempt id survives send_payment's custom records and list_payments' JSON"
    (is (= {"0xf1be" "0x000000000000002a"} (client/attempt-records 42)))
    (is (= {:hash "0xab", :id 42, :payment-status "Success", :fee 3, :error nil}
           (client/listed-payment
             (json/parse-string
               (str "{\"payment_hash\":\"0xab\",\"status\":\"Success\",\"fee\":\"0x3\","
                    "\"failed_error\":null,\"custom_records\":{\"0xf1be\":\"0x000000000000002a\"}}")
               true))))
    (is (nil? (:id (client/listed-payment {:payment_hash "0xab", :status "Failed", :fee "0x0"}))))))

(deftest balanced
  ; n1 pays n2 100; n1 pays n3 50 through n2 for a fee of 1; n3's payment to n1 fails.
  (let [r (check (line-channels 849 651 950 550)
                 {"n1" [(listed "0xa1" 1 "Success" 0) (listed "0xa2" 2 "Success" 1)]
                  "n2" []
                  "n3" [(listed "0xa3" 3 "Failed" 0)]}
                 [(pay :ok 1 "n1" "n2" 100 {:hash "0xa1", :fee 0})
                  (pay :ok 2 "n1" "n3" 50 {:hash "0xa2", :fee 1})
                  (pay :fail 3 "n3" "n1" 30 {:hash "0xa3"})])]
    (is (= true (:valid? r)))
    (is (= :balanced (:outcome r)))
    (is (= {"n1" -151, "n2" 101, "n3" 50} (update-vals (:nodes r) :change)))
    (is (= {"n1" -151, "n2" 101, "n3" 50} (update-vals (:nodes r) :expected)))
    (is (= {:listed 3, :success 2, :failed 1, :unfinished 0, :found-by-listing 0} (:payments r)))
    (is (= {} (:incomplete r)))))

(deftest a-removed-tlc-still-listed-is-settled
  ; As balanced, but the last removal on each channel is still listed, in status
  ; RemoveAckConfirmed, because nothing has updated the channels since.
  (let [done  (fn [dir] [{:payment_hash "0xa2", :amount "0x32", :status {dir "RemoveAckConfirmed"}}])
        final (-> (line-channels 849 651 950 550)
                  (assoc-in ["n1" 0 :pending_tlcs] (done :Outbound))
                  (assoc-in ["n2" 0 :pending_tlcs] (done :Inbound))
                  (assoc-in ["n2" 1 :pending_tlcs] (done :Outbound))
                  (assoc-in ["n3" 0 :pending_tlcs] (done :Inbound)))
        ops   [(pay :ok 1 "n1" "n2" 100 {:hash "0xa1", :fee 0})
               (pay :ok 2 "n1" "n3" 50 {:hash "0xa2", :fee 1})
               (pay :fail 3 "n3" "n1" 30 {:hash "0xa3"})]
        list  {"n1" [(listed "0xa1" 1 "Success" 0) (listed "0xa2" 2 "Success" 1)]
               "n2" []
               "n3" [(listed "0xa3" 3 "Failed" 0)]}]
    (is (= true (:valid? (check final list ops))))
    (is (= {} (:incomplete (check final list ops))))
    (testing "a TLC still in flight on one end is noted as possibly unsettled"
      (let [r (check (assoc-in final ["n2" 1 :pending_tlcs 0 :status] {:Outbound "RemoveWaitAck"})
                     list ops)]
        (is (= [{:node "n2", :channel "0x23", :state "ChannelReady", :tlcs 1}]
               (get-in r [:incomplete :channels])))))))

(deftest forwarder-pays-but-the-sender-fails
  ; n2 settles the forwarded TLC with n3, but the TLC from n1 fails: n1 sees Failed.
  (let [final (line-channels 900 600 950 550)
        r     (check final
                     {"n1" [(listed "0xa1" 1 "Success" 0) (listed "0xa2" 2 "Failed" 0)]
                      "n2" []
                      "n3" []}
                     [(pay :ok 1 "n1" "n2" 100 {:hash "0xa1", :fee 0})
                      (pay :fail 2 "n1" "n3" 50 {:hash "0xa2"})])]
    (testing "every per-channel check passes"
      (is (= [] (fc/channel-problems initial final))))
    (is (= false (:valid? r)))
    (is (= :unbalanced (:outcome r)))
    (is (= ["n2" "n3"] (:unbalanced-nodes r)))
    (is (= {"n1" 0, "n2" -50, "n3" 50} (update-vals (:nodes r) :diff)))))

(deftest a-payment-the-client-never-heard-back-from
  ; send_payment timed out, so the client has no hash, but n1 made the payment.
  (let [r (check (line-channels 980 520 1000 500)
                 {"n1" [(listed "0xa4" 4 "Success" 0)], "n2" [], "n3" []}
                 [(pay :info 4 "n1" "n2" 20 {:error [:no-reply "SocketTimeoutException"]})])]
    (is (= :balanced (:outcome r)))
    (is (= 1 (get-in r [:payments :found-by-listing])))))

(deftest told-no-payment-was-created-but-one-succeeded
  (let [r (check (line-channels 980 520 1000 500)
                 {"n1" [(listed "0xa5" 5 "Success" 0)], "n2" [], "n3" []}
                 [(pay :fail 5 "n1" "n2" 20 {:error "send_payment: some rejection"})])]
    (is (= false (:valid? r)))
    (is (= :client-mismatch (:outcome r)))
    (is (= [{:id 5, :from "n1", :to "n2", :amount 20, :source :pay, :client :none
             :final-statuses ["Success"]}]
           (:status-mismatches r)))))

(deftest a-success-that-becomes-failed
  (let [r (check initial
                 {"n1" [(listed "0xa6" 6 "Failed" 0)], "n2" [], "n3" []}
                 [(pay :ok 6 "n1" "n2" 20 {:hash "0xa6", :fee 0})])]
    (is (= :client-mismatch (:outcome r)))
    (is (= ["Failed"] (:final-statuses (first (:status-mismatches r)))))))

(deftest one-attempt-two-payments
  (let [r (check (line-channels 960 540 1000 500)
                 {"n1" [(listed "0xa7" 7 "Success" 0) (listed "0xb7" 7 "Success" 0)], "n2" [], "n3" []}
                 [(pay :ok 7 "n1" "n2" 20 {:hash "0xa7", :fee 0})])]
    (is (= false (:valid? r)))
    (is (= 7 (:id (first (:duplicate-attempts r)))))))

(deftest a-difference-is-unknown-when-records-cannot-show-everything
  (let [ops [(pay :ok 1 "n1" "n2" 100 {:hash "0xa1", :fee 0})
             (pay :info 2 "n1" "n3" 50 {:hash "0xa2"})]]
    (testing "a closed channel: settlement may have moved on chain"
      (let [final (-> (line-channels 900 600 950 550)
                      (assoc-in ["n2" 1 :state :state_name] "Closed")
                      (assoc-in ["n3" 0 :state :state_name] "Closed"))
            r     (check final
                         {"n1" [(listed "0xa1" 1 "Success" 0) (listed "0xa2" 2 "Failed" 0)]
                          "n2" [], "n3" []}
                         ops)]
        (is (= :unknown (:valid? r)))
        (is (= #{"n2" "n3"} (set (map :node (get-in r [:incomplete :channels])))))))
    (testing "a payment still in flight"
      (let [r (check (line-channels 900 600 950 550)
                     {"n1" [(listed "0xa1" 1 "Success" 0) (listed "0xa2" 2 "Inflight" 0)]
                      "n2" [], "n3" []}
                     ops)]
        (is (= :unknown (:valid? r)))
        (is (= [{:node "n1", :hash "0xa2", :id 2, :payment-status "Inflight"}]
               (get-in r [:incomplete :unfinished-payments])))))
    (testing "no listing in the final read, or a node that can't list"
      (is (= :unknown (:valid? (ledger/analyze nodes initial {:channels initial} ops))))
      (is (= :unknown (:valid? (check initial {"n1" {:error "Method not found"}, "n2" [], "n3" []}
                                      ops)))))))

(deftest a-payment-the-sender-no-longer-lists
  (let [r (check initial {"n1" [], "n2" [], "n3" []}
                 [(pay :info 8 "n1" "n2" 20 {:hash "0xa8"})])]
    (is (= :balanced (:outcome r)))
    (is (= [{:id 8, :from "n1", :to "n2", :hash "0xa8", :client nil}]
           (get-in r [:incomplete :not-listed])))))

(deftest probe-payments-count-too
  (let [probe {:type  :ok
               :f     :probe
               :value [{:from "n1", :to "n2", :amount 10, :id 11, :hash "0xp1", :status "Success"}
                       {:from "n3", :to "n2", :amount 10, :id 12, :status "Rejected", :error ":node-down"}]}
        r     (check (line-channels 990 510 1000 500)
                     {"n1" [(listed "0xp1" 11 "Success" 0)], "n2" [], "n3" []}
                     [probe])]
    (is (= :balanced (:outcome r)))
    (is (= {"n1" -10, "n2" 10, "n3" 0} (update-vals (:nodes r) :change)))))

(deftest the-checker-reads-the-final-read-from-the-history
  (let [test {:nodes nodes, :initial-channels (atom initial)}
        read {:type  :ok
              :f     :read
              :value {:channels (line-channels 900 600 950 550)
                      :listed   {"n1" [(listed "0xa1" 1 "Success" 0)], "n2" [], "n3" []}}}
        r    (checker/check (ledger/checker) test
                            [(pay :ok 1 "n1" "n2" 100 {:hash "0xa1", :fee 0}) read] {})]
    ; n2 paid n3 50 that no payment explains.
    (is (= :unbalanced (:outcome r)))))

;; Off the line: a ring n1 - n2 - n3 - n4 - n1, where a payment may take either way.

(def ring-nodes ["n1" "n2" "n3" "n4"])

(def ring-ids
  {"0x12" ["n1" "n2"], "0x23" ["n2" "n3"], "0x34" ["n3" "n4"], "0x41" ["n4" "n1"]})

(defn ring-channels
  "Channel records for the ring: each channel starts 1000 on both ends, and
  `moves` maps a channel id to how much its first node paid its second."
  [moves]
  (reduce (fn [m [id [a b]]]
            (let [d (get moves id 0)]
              (-> m
                  (update a (fnil conj []) (channel id (- 1000 d) (+ 1000 d)))
                  (update b (fnil conj []) (channel id (+ 1000 d) (- 1000 d))))))
          {}
          (sort ring-ids)))

(defn ring-check
  [moves listing ops]
  (ledger/analyze ring-nodes (ring-channels {}) {:channels (ring-channels moves), :listed listing}
                  ops {:line? false}))

(def no-listing {"n2" [], "n3" [], "n4" []})

(deftest off-the-line
  (testing "n1 pays n3 50 for a fee of 1, through either n2 or n4"
    (doseq [moves [{"0x12" 51, "0x23" 50} {"0x41" -51, "0x34" -50}]]
      (let [r (ring-check moves
                          (assoc no-listing "n1" [(listed "0xa1" 1 "Success" 1)])
                          [(pay :ok 1 "n1" "n3" 50 {:hash "0xa1", :fee 1})])]
        (is (= :balanced (:outcome r)))
        (is (= {"n1" -51, "n2" 0, "n3" 50, "n4" 0} (update-vals (:nodes r) :expected)))
        (is (= {"n1" 0, "n2" 1, "n3" 0, "n4" 1} (update-vals (:nodes r) :forwarded-max)))
        (is (= {:paid 1, :forwarded 1} (:fees r))))))
  (testing "neighbours: no fee, nothing forwarded"
    (let [r (ring-check {"0x12" 20}
                        (assoc no-listing "n1" [(listed "0xa1" 1 "Success" 0)])
                        [(pay :ok 1 "n1" "n2" 20 {:hash "0xa1", :fee 0})])]
      (is (= :balanced (:outcome r)))))
  (testing "n2 pays n3 for a payment from n1 that failed"
    (let [r (ring-check {"0x23" 50}
                        (assoc no-listing "n1" [(listed "0xa1" 1 "Failed" 0)])
                        [(pay :fail 1 "n1" "n3" 50 {:hash "0xa1"})])]
      (is (= false (:valid? r)))
      (is (= :unbalanced (:outcome r)))
      (is (= ["n2" "n3"] (:unbalanced-nodes r)))
      (is (= {"n1" 0, "n2" -50, "n3" 50, "n4" 0} (update-vals (:nodes r) :diff)))))
  (testing "n2 is paid for a payment it doesn't pass on"
    (let [r (ring-check {"0x12" 51}
                        (assoc no-listing "n1" [(listed "0xa1" 1 "Success" 1)])
                        [(pay :ok 1 "n1" "n3" 50 {:hash "0xa1", :fee 1})])]
      (is (= :unbalanced (:outcome r)))
      (is (= ["n2" "n3"] (:unbalanced-nodes r)))))
  (testing "income within every node's bound, but not adding up to the fees"
    ; n1 pays n3 through n2 for a fee of 2, and n4's end of 0x34 gains 2 from nowhere
    ; (the channel checks catch that too).
    (let [final (-> (ring-channels {"0x12" 52, "0x23" 50})
                    (update "n4" (partial mapv #(if (= "0x34" (:channel_id %))
                                                  (channel "0x34" 1002 1000)
                                                  %))))
          r     (ledger/analyze ring-nodes (ring-channels {})
                                {:channels final
                                 :listed   (assoc no-listing "n1" [(listed "0xa1" 1 "Success" 2)])}
                                [(pay :ok 1 "n1" "n3" 50 {:hash "0xa1", :fee 2})]
                                {:line? false})]
      (is (= [] (:unbalanced-nodes r)))
      (is (= {:paid 2, :forwarded 4} (:fees r)))
      (is (= :unbalanced (:outcome r)))))
  (testing "the line keeps its exact check"
    (is (nil? (:fees (check (line-channels 1000 500 1000 500) {"n1" [], "n2" [], "n3" []} []))))))
