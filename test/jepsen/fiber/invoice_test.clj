(ns jepsen.fiber.invoice-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [client :as client]
                          [invoice :as invoice]
                          [rpc :as rpc]
                          [schedule :as schedule]])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetSocketAddress)))

(def nodes ["n1" "n2" "n3"])

(defn- pays
  [seed kinds]
  (take 300 (schedule/with-kinds (schedule/kinds-stream (schedule/streams seed) :pay) kinds
                                 (schedule/payments (:pay (schedule/streams seed)) 3))))

(deftest kinds
  (testing "keysend alone draws nothing and adds no :kind"
    (is (= (take 300 (schedule/payments (:pay (schedule/streams 5)) 3))
           (pays 5 [:keysend]))))
  (testing "one kind: every payment, nothing drawn"
    (is (every? #(= :invoice (get-in % [:value :kind])) (pays 5 [:invoice]))))
  (testing "a mix, drawn from a stream of its own"
    (let [mixed (pays 5 [:keysend :invoice])
          freq  (frequencies (map (comp :kind :value) mixed))]
      (is (< 100 (:invoice freq) 200))
      (is (< 100 (:keysend freq) 200))
      (is (= (map #(update % :value dissoc :kind) mixed) (pays 5 [:keysend])))
      (is (= mixed (pays 5 [:keysend :invoice])))
      (is (not= mixed (pays 6 [:keysend :invoice])))))
  (testing "a kind listed twice is drawn twice as often"
    (let [freq (frequencies (map (comp :kind :value) (pays 5 [:keysend :invoice :invoice])))]
      (is (< 150 (:invoice freq) 250)))))

(deftest the-fault-window-keeps-its-payments
  (let [opts  {:nodes nodes, :rate 5, :time-limit 60, :nemesis-interval 10
               :kill-targets [:one :all], :faults [:kill]}
        run   (fn [o] (->> (gt/simulate (gen/context {:concurrency 3})
                                        (fiber/fault-window o (schedule/streams 9))
                                        (fn [_ op] (-> op (assoc :type :ok) (update :time + 1000000))))
                           (filter #(#{:invoke :info} (:type %)))
                           (map (juxt :f #(if (= :pay (:f %)) (dissoc (:value %) :kind) (:value %))))))
        plain (run opts)
        mixed (run (assoc opts :pay-kinds [:keysend :invoice]))]
    (is (< 250 (count plain)))
    (is (= plain mixed))))

(deftest preimage
  (is (re-matches #"0x[0-9a-f]{64}" (invoice/preimage 42)))
  (is (= (invoice/preimage 42) (invoice/preimage 42)))
  (is (not= (invoice/preimage 42) (invoice/preimage 43)))
  (is (not= (invoice/preimage nil) (invoice/preimage nil))))

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

(deftest pays-an-invoice
  (let [calls  (atom [])
        server (fake-fnn calls (fn [m _]
                                 (case m
                                   "new_invoice"  {:invoice_address "fibd1fake"
                                                   :invoice {:data {:payment_hash "0xab"}}}
                                   "send_payment" {:payment_hash "0xab", :status "Created"}
                                   "get_payment"  {:payment_hash "0xab", :status "Success", :fee "0x3e8"})))
        port   (.getPort (.getAddress server))
        test   {:nodes nodes, :pubkeys (atom {}), :payments (atom []), :ledger true}
        op     {:type :invoke, :f :pay, :value {:id 7, :amount 2000000, :to "n3", :kind :invoice}}]
    (try
      (with-redefs [rpc/node-url (fn [node]
                                   (if (= "n2" node)
                                     "http://127.0.0.1:1"    ; nothing listens: the receiver is down
                                     (str "http://127.0.0.1:" port)))]
        (testing "the receiver makes the invoice, the sender pays it"
          (let [done (client/pay! test "n1" op)]
            (is (= :ok (:type done)))
            (is (= {:id 7, :amount 2000000, :to "n3", :from "n1", :kind :invoice, :hash "0xab", :fee 1000}
                   (:value done)))
            (let [[[m1 p1] [m2 p2]] @calls]
              (is (= "new_invoice" m1))
              (is (= {:amount "0x1e8480", :currency "Fibd", :expiry "0xe10"}
                     (select-keys p1 [:amount :currency :expiry])))
              (is (= (invoice/preimage 7) (:payment_preimage p1)))
              (is (= "send_payment" m2))
              (testing "with the attempt id, for --ledger"
                (is (= {:invoice "fibd1fake", :custom_records {(keyword "0xf1be") "0x0000000000000007"}}
                       p2))))))
        (testing "a receiver that is down: no invoice, no payment"
          (reset! calls [])
          (let [done (client/pay! test "n1" (assoc-in op [:value :to] "n2"))]
            (is (= :fail (:type done)))
            (is (= [:new-invoice :receiver-down] (:error done)))
            (is (empty? @calls)))))
      (finally (.stop server 0)))))

(deftest options
  (let [parse #(cli/parse-opts % fiber/cli-opts)]
    (is (= [:keysend] (:pay-kinds (:options (parse [])))))
    (is (= [:keysend :invoice] (:pay-kinds (:options (parse ["--pay-kinds" "keysend,invoice"])))))
    (is (seq (:errors (parse ["--pay-kinds" "keysend,mpp"]))))))
