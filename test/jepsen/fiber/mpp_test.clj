(ns jepsen.fiber.mpp-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [client :as client]
                          [invoice :as invoice]
                          [mpp :as mpp]
                          [rpc :as rpc]
                          [schedule :as schedule]
                          [topology :as topology]])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetSocketAddress)))

(def nodes ["n1" "n2" "n3" "n4"])

(def ckb 100000000)

(deftest amounts
  (testing "the 20 amounts payments draw spread evenly over the range"
    (is (= (* 1000 ckb) (schedule/scale-amount [(* 1000 ckb) (* 1600 ckb)] 1000000)))
    (is (= (* 1600 ckb) (schedule/scale-amount [(* 1000 ckb) (* 1600 ckb)] 20000000)))
    (is (= 20 (count (set (map #(schedule/scale-amount [(* 1000 ckb) (* 1600 ckb)] (* % 1000000))
                               (range 1 21)))))))
  (testing "only MPP payments take the range; nothing else changes"
    (let [pays   (fn [ranges]
                   (take 300 (schedule/with-amounts
                               ranges
                               (schedule/with-kinds (schedule/kinds-stream (schedule/streams 5) :pay)
                                                    [:keysend :mpp]
                                                    (schedule/payments (:pay (schedule/streams 5)) 4)))))
          plain  (pays nil)
          scaled (pays (mpp/amount-ranges {:pay-kinds [:keysend :mpp], :mpp-amount [1000 1600]}))]
      (is (= (map #(update % :value dissoc :amount) plain)
             (map #(update % :value dissoc :amount) scaled)))
      (doseq [[p s] (map vector plain scaled)]
        (if (= :mpp (get-in p [:value :kind]))
          (is (<= (* 1000 ckb) (get-in s [:value :amount]) (* 1600 ckb)))
          (is (= p s))))))
  (testing "no ranges without mpp"
    (is (nil? (mpp/amount-ranges {:pay-kinds [:keysend :invoice]})))))

(deftest the-fault-window-keeps-its-payments
  (testing "adding mpp to a mix changes only the kinds and amounts drawn, not ids or destinations"
    (let [opts (fn [kinds] {:nodes nodes, :rate 5, :time-limit 60, :nemesis-interval 10
                            :faults [:packet], :pay-kinds kinds})
          run  (fn [o] (->> (gt/simulate (gen/context {:concurrency 4})
                                         (fiber/fault-window o (schedule/streams 9))
                                         (fn [_ op] (-> op (assoc :type :ok) (update :time + 1000000))))
                            (filter #(#{:invoke :info} (:type %)))
                            (map (juxt :f #(if (= :pay (:f %))
                                             (dissoc (:value %) :kind :amount)
                                             (:value %))))))]
      (is (= (run (opts [:keysend :invoice])) (run (opts [:keysend :mpp])))))))

(defn- fake-fnn
  "An HTTP server answering fnn's JSON-RPC calls with `answer` (fn [method
  params] -> result), recording each call in `calls`."
  [calls answer]
  (doto (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)
    (.createContext "/" (reify HttpHandler
                          (handle [_ ex]
                            (let [^HttpExchange ex ex
                                  req  (json/parse-string (slurp (.getRequestBody ex)) true)
                                  m    (:method req)
                                  p    (first (:params req))
                                  _    (swap! calls conj [m p])
                                  body (.getBytes (json/generate-string
                                                    {:jsonrpc "2.0", :id 1, :result (answer m p)}))]
                              (.sendResponseHeaders ex 200 (count body))
                              (with-open [o (.getResponseBody ex)] (.write o body))))))
    (.start)))

(deftest pays-in-parts
  (let [calls  (atom [])
        polls  (atom 0)
        tlc    (fn [id amount] {:id (rpc/int->hex id), :amount (rpc/int->hex amount)
                                :payment_hash "0xab", :status {:Outbound "Committed"}})
        server (fake-fnn calls
                         (fn [m _]
                           (case m
                             "new_invoice"   {:invoice_address "fibd1mpp"
                                              :invoice {:data {:payment_hash "0xab"}}}
                             "send_payment"  {:payment_hash "0xab", :status "Created"}
                             ; Two parts in flight on two channels, then one left, then settled.
                             "list_channels" (case (swap! polls inc)
                                               1 {:channels [{:channel_id "0xc1", :pending_tlcs [(tlc 0 600)]}
                                                             {:channel_id "0xc2", :pending_tlcs [(tlc 3 500)]}]}
                                               2 {:channels [{:channel_id "0xc1", :pending_tlcs [(tlc 0 600)]}
                                                             {:channel_id "0xc2", :pending_tlcs [(tlc 4 7 )]}]}
                                               {:channels [{:channel_id "0xc1", :pending_tlcs []}]})
                             "get_payment"   {:payment_hash "0xab"
                                              :status (if (< 2 @polls) "Success" "Inflight")
                                              :fee "0x3e8"})))
        port   (.getPort (.getAddress server))
        test   {:nodes nodes, :pubkeys (atom {}), :payments (atom []), :invoices (atom []), :ledger true
                :mpp-max-parts 4}
        op     {:type :invoke, :f :pay, :value {:id 7, :amount 1100, :to "n3", :kind :mpp}}]
    (try
      (with-redefs [rpc/node-url (fn [_] (str "http://127.0.0.1:" port))]
        (let [done (client/pay! test "n1" op)]
          (is (= :ok (:type done)))
          (testing "the invoice allows MPP and the payment may take up to max_parts"
            (let [params (fn [m] (some (fn [[m' p]] (when (= m m') p)) @calls))]
              (is (true? (:allow_mpp (params "new_invoice"))))
              (is (= (invoice/preimage 7) (:payment_preimage (params "new_invoice"))))
              (is (= {:invoice "fibd1mpp", :max_parts "0x4"
                      :custom_records {(keyword "0xf1be") "0x0000000000000007"}}
                     (params "send_payment")))))
          (testing "the parts the sender's channels showed"
            (is (= {:max-at-once 2, :max-amount 1100, :tlcs 3, :channels 2}
                   (get-in done [:value :parts])))
            (is (= "0xab" (get-in done [:value :invoice-hash] )))
            (is (= "0xab" (get-in done [:value :hash])))
            (is (= 1000 (get-in done [:value :fee]))))
          (testing "the invoice is remembered for the final read"
            (is (= [{:id 7, :from "n1", :to "n3", :amount 1100, :hash "0xab"}] @(:invoices test))))))
      (finally (.stop server 0)))))

(deftest reads-both-ends
  (let [calls  (atom [])
        server (fake-fnn calls (fn [m p]
                                 (case m
                                   "get_invoice" {:status "Paid", :invoice_address "fibd1mpp"}
                                   "get_payment" {:status "Success", :fee "0x10"})))
        port   (.getPort (.getAddress server))
        test   {:invoices (atom [{:id 7, :from "n1", :to "n3", :amount 1100, :hash "0xab"}])}]
    (try
      (with-redefs [rpc/node-url (fn [node] (str "http://127.0.0.1:" port "/" node))]
        (let [done (client/invoke-op test "n1" {:type :invoke, :f :read-invoices})]
          (is (= :ok (:type done)))
          (is (= [{:id 7, :from "n1", :to "n3", :amount 1100, :hash "0xab"
                   :invoice-status "Paid", :payment-status "Success", :fee 16}]
                 (:value done)))
          (is (= #{["get_invoice" {:payment_hash "0xab"}] ["get_payment" {:payment_hash "0xab"}]}
                 (set @calls)))))
      (finally (.stop server 0)))))

(deftest outcomes
  (let [o #(mpp/outcome (merge {:invoice-status %1, :payment-status %2} %3))]
    (is (= :paid (o "Paid" "Success" nil)))
    (is (= :unpaid (o "Open" "Failed" nil)))
    (is (= :unpaid (o "Cancelled" "Failed" nil)))
    (is (= :unpaid (o "Open" nil {:no-payment true, :payment-error "Payment session not found"})))
    (is (= :paid-but-failed (o "Paid" "Failed" nil)))
    (is (= :paid-but-failed (o "Paid" nil {:no-payment true})))
    (is (= :success-but-unpaid (o "Open" "Success" nil)))
    (is (= :held (o "Received" "Failed" nil)))
    (is (= :held (o "Received" "Success" nil)))
    (is (= :unfinished (o "Open" "Inflight" nil)))
    (is (= :unread (o nil "Success" nil)))
    (is (= :unread (o "Paid" nil {:payment-unread "Connection refused"})))))

(defn- pay
  [type id parts]
  {:type type, :f :pay, :value (cond-> {:id id, :kind :mpp, :amount 1100}
                                 parts (assoc :parts parts))})

(defn- row
  [id invoice payment]
  {:id id, :from "n1", :to "n2", :amount 1100, :hash (str "0x" id)
   :invoice-status invoice, :payment-status payment})

(deftest checker
  (let [split {:max-at-once 2, :max-amount 1100, :tlcs 2, :channels 2}
        one   {:max-at-once 1, :max-amount 1100, :tlcs 1, :channels 1}
        base  [(pay :ok 1 split) (pay :ok 2 one) (pay :fail 3 nil) (pay :info 4 nil)
               {:type :fail, :f :pay, :value {:id 5, :kind :mpp}, :error [:new-invoice :receiver-down]}
               {:type :ok, :f :pay, :value {:id 6, :amount 5}}]
        read  (fn [rows] [{:type :ok, :f :read-invoices, :value rows}])
        good  [(row 1 "Paid" "Success") (row 2 "Paid" "Success") (row 3 "Open" "Failed")
               (row 4 "Cancelled" "Failed")]]
    (testing "all or nothing"
      (let [r (mpp/analyze (concat base (read good)))]
        (is (true? (:valid? r)))
        (is (= 5 (:attempts r)))
        (is (= 4 (:invoices r)))
        (is (= 1 (:no-invoice r)))
        (is (= {:paid 2, :unpaid 2} (:outcomes r)))
        (is (= {:split 1, :one-part 1} (:split r)))))
    (testing "the ends disagree"
      (let [r (mpp/analyze (concat base (read (assoc good 3 (row 4 "Paid" "Failed")))))]
        (is (false? (:valid? r)))
        (is (= [4] (map :id (:disagree r))))))
    (testing "not finished"
      (let [r (mpp/analyze (concat base (read (assoc good 2 (row 3 "Received" "Inflight")))))]
        (is (= :unknown (:valid? r)))
        (is (= [3] (map :id (:not-final r))))))
    (testing "without the read, or with nothing to read"
      (is (= :unknown (:valid? (mpp/analyze base))))
      (is (= :unknown (:valid? (mpp/analyze (read []))))))))

(deftest options
  (let [parse #(cli/parse-opts % fiber/cli-opts)]
    (testing "defaults"
      (let [o (:options (parse []))]
        (is (= [1000 1600] (:mpp-amount o)))
        (is (nil? (:mpp-max-parts o)))
        (is (nil? (:acceptor-funding o)))))
    (is (= [500 900] (:mpp-amount (:options (parse ["--mpp-amount" "500-900"])))))
    (is (seq (:errors (parse ["--mpp-amount" "900-500"]))))
    (is (= 6 (:mpp-max-parts (:options (parse ["--mpp-max-parts" "6"])))))
    (is (seq (:errors (parse ["--mpp-max-parts" "1"]))))
    (is (seq (:errors (parse ["--acceptor-funding" "50"]))))
    (testing "acceptor funding reaches fnn's environment"
      (is (nil? (topology/fnn-env {})))
      (is (= {:FIBER_AUTO_ACCEPT_CHANNEL_CKB_FUNDING_AMOUNT "100000000000"}
             (topology/fnn-env (:options (parse ["--acceptor-funding" "1000"]))))))))
