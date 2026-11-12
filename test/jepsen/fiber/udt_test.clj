(ns jepsen.fiber.udt-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [jepsen.fiber [client :as client]
                          [invoice]
                          [rpc :as rpc]
                          [udt :as udt]])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetSocketAddress)))

; No test leaves the token set for the next one.
(use-fixtures :each (fn [f] (try (f) (finally (reset! udt/script nil)))))

(def nodes ["n1" "n2" "n3"])

(def token
  {:code_hash "0xe1e3", :hash_type "data2", :args udt/simple-udt-args})

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

(defn- answer
  [m _]
  (case m
    "node_info"    {:udt_cfg_infos [{:name "XUDT", :script {:code_hash "0x50bd", :hash_type "Data2", :args "0x.*"}}
                                    {:name "SIMPLE_UDT", :script {:code_hash "0xe1e3", :hash_type "Data2", :args "0x.*"}}]}
    "new_invoice"  {:invoice_address "fibd1udt"}
    "send_payment" {:payment_hash "0xab", :status "Created"}
    "get_payment"  {:payment_hash "0xab", :status "Success", :fee "0x0"}))

(deftest the-token
  (let [calls  (atom [])
        server (fake-fnn calls answer)
        port   (.getPort (.getAddress server))
        test   {:nodes nodes, :pubkeys (atom {"n3" "0x03"}), :payments (atom [])}
        op     {:type :invoke, :f :pay, :value {:id 7, :amount 2000000, :to "n3"}}
        params (fn [m] (some (fn [[m' p]] (when (= m m') p)) @calls))]
    (try
      (with-redefs [rpc/node-url (fn [_] (str "http://127.0.0.1:" port))]
        (testing "without --asset udt nothing is read, and no call names a token"
          (is (nil? (udt/setup! test)))
          (is (empty? @calls))
          (is (nil? (udt/payment-params)))
          (is (nil? (udt/channel-params)))
          (client/pay! test "n1" op)
          (client/pay! test "n1" (assoc-in op [:value :kind] :invoice))
          (is (not-any? #(contains? (second %) :udt_type_script) @calls)))
        (testing "with it, setup reads the simple UDT from the first node's whitelist"
          (reset! calls [])
          (is (= token (udt/setup! (assoc test :asset :udt))))
          (is (= [["node_info" nil]] @calls))
          (is (= token @udt/script)))
        (testing "a channel is opened with the token, as many units as a CKB channel has shannons"
          (is (= {:funding_amount "0x174876e800", :funding_udt_type_script token}
                 (udt/channel-params))))
        (testing "a keysend pays in the token"
          (reset! calls [])
          (client/pay! test "n1" op)
          (is (= {:target_pubkey "0x03", :amount "0x1e8480", :keysend true, :udt_type_script token}
                 (params "send_payment"))))
        (testing "an invoice is for the token, and so is its payment"
          (reset! calls [])
          (client/pay! test "n1" (assoc-in op [:value :kind] :invoice))
          (is (= token (:udt_type_script (params "new_invoice"))))
          (is (= {:invoice "fibd1udt", :udt_type_script token} (params "send_payment"))))
        (testing "the next test's setup clears it"
          (is (nil? (udt/setup! test)))
          (is (nil? @udt/script))))
      (finally (.stop server 0)))))

(deftest a-node-without-the-token-in-its-whitelist
  (let [server (fake-fnn (atom []) (fn [_ _] {:udt_cfg_infos [{:name "XUDT"}]}))
        port   (.getPort (.getAddress server))]
    (try
      (with-redefs [rpc/node-url (fn [_] (str "http://127.0.0.1:" port))]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"no SIMPLE_UDT"
                              (udt/setup! {:nodes nodes, :asset :udt}))))
      (finally (.stop server 0)))))

(deftest options
  (let [opts {:asset :udt, :nodes nodes, :pay-kinds [:keysend :invoice]}]
    (is (nil? (udt/check-opts! opts)))
    (is (nil? (udt/check-opts! {:asset :ckb, :nodes ["n1" "n4"], :onchain true})))
    (testing "only n1-n3 hold the token"
      (is (thrown-with-msg? IllegalArgumentException #"n1, n2, n3"
                            (udt/check-opts! (assoc opts :nodes ["n1" "n2" "n4"])))))
    (testing "not with what counts in CKB"
      (doseq [[k v] [[:onchain true] [:cch true] [:acceptor-funding 1000] [:pay-kinds [:mpp]]]]
        (is (thrown? IllegalArgumentException (udt/check-opts! (assoc opts k v))))))))
