(ns jepsen.fiber.trampoline-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [client :as client]
                          [invoice :as invoice]
                          [rpc :as rpc]
                          [schedule :as schedule]
                          [trampoline :as trampoline]])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetSocketAddress)))

(def nodes ["n1" "n2" "n3" "n4" "n5" "n6"])

(deftest hops
  (let [test {:nodes nodes}]
    (testing "one of the other nodes, from the attempt id alone"
      (let [vias (map #(trampoline/hops test "n1" "n3" %) (range 400))]
        (is (= vias (map #(trampoline/hops (assoc test :seed 5) "n1" "n3" %) (range 400))))
        (is (every? #(= 1 (count %)) vias))
        (is (= #{"n2" "n4" "n5" "n6"} (set (map first vias))))))
    (testing "a range of counts, without sender, receiver or repeats"
      (let [vias (map #(trampoline/hops (assoc test :trampoline-hops [1 3]) "n1" "n3" %) (range 400))]
        (is (= #{1 2 3} (set (map count vias))))
        (is (every? #(apply distinct? %) vias))
        (is (not-any? #(some #{"n1" "n3"} %) vias))
        (testing "in either order"
          (is (some #{["n2" "n4"]} vias))
          (is (some #{["n4" "n2"]} vias)))))
    (testing "no more than there are"
      (is (= ["n2"] (trampoline/hops {:nodes ["n1" "n2" "n3"], :trampoline-hops [2 2]} "n1" "n3" 7)))
      (is (= [] (trampoline/hops {:nodes ["n1" "n2"]} "n1" "n2" 7))))))

(deftest fee-budget
  (is (= 20000 (trampoline/max-fee 2000000 1)))
  (is (= 40000 (trampoline/max-fee 2000000 2)))
  (is (= 1 (trampoline/max-fee 10 1))))

(deftest the-fault-window-keeps-its-payments
  (testing "trampoline in the mix changes the kinds drawn, not ids, amounts or destinations"
    (let [opts (fn [kinds] {:nodes nodes, :rate 5, :time-limit 60, :nemesis-interval 10
                            :kill-targets [:one], :faults [:kill], :pay-kinds kinds})
          run  (fn [o] (->> (gt/simulate (gen/context {:concurrency 6})
                                         (fiber/fault-window o (schedule/streams 9))
                                         (fn [_ op] (-> op (assoc :type :ok) (update :time + 1000000))))
                            (filter #(#{:invoke :info} (:type %)))
                            (map (juxt :f #(if (= :pay (:f %)) (dissoc (:value %) :kind) (:value %))))))
          a    (run (opts [:keysend :invoice]))
          b    (run (opts [:keysend :trampoline]))]
      (is (< 250 (count a)))
      (is (= a b)))))

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

(deftest pays-by-trampoline
  (let [calls  (atom [])
        server (fake-fnn calls (fn [m _]
                                 (case m
                                   "new_invoice"  {:invoice_address "fibd1tramp"
                                                   :invoice {:data {:payment_hash "0xab"}}}
                                   "send_payment" {:payment_hash "0xab", :status "Created"}
                                   "get_payment"  {:payment_hash "0xab", :status "Success", :fee "0x4e20"})))
        port   (.getPort (.getAddress server))
        test   {:nodes   nodes
                :pubkeys (atom (into {} (for [n nodes] [n (str "0x02" n)])))
                :payments (atom []), :ledger true, :trampoline-hops [2 2]}
        op     {:type :invoke, :f :pay, :value {:id 7, :amount 2000000, :to "n3", :kind :trampoline}}
        via    (trampoline/hops test "n1" "n3" 7)
        params (fn [m] (some (fn [[m' p]] (when (= m m') p)) @calls))]
    (try
      (with-redefs [rpc/node-url (fn [_] (str "http://127.0.0.1:" port))]
        (let [done (client/pay! test "n1" op)]
          (is (= :ok (:type done)))
          (testing "the invoice allows trampoline routing, and is an ordinary one otherwise"
            (is (true? (:allow_trampoline_routing (params "new_invoice"))))
            (is (= (invoice/preimage 7) (:payment_preimage (params "new_invoice")))))
          (testing "the payment names its trampoline nodes by pubkey and its fee budget"
            (is (= 2 (count via)))
            (is (= {:invoice         "fibd1tramp"
                    :trampoline_hops (mapv #(str "0x02" %) via)
                    :max_fee_amount  "0x9c40"
                    :custom_records  {(keyword "0xf1be") "0x0000000000000007"}}
                   (params "send_payment"))))
          (testing "the op records them"
            (is (= via (get-in done [:value :via])))
            (is (= 20000 (get-in done [:value :fee]))))))
      (testing "two nodes: nobody to go through, nothing sent"
        (reset! calls [])
        (with-redefs [rpc/node-url (fn [_] (str "http://127.0.0.1:" port))]
          (let [done (client/pay! (assoc test :nodes ["n1" "n3"]) "n1" op)]
            (is (= :fail (:type done)))
            (is (= :no-trampoline-node (:error done)))
            (is (empty? @calls)))))
      (finally (.stop server 0)))))

(deftest checker
  (let [pay (fn [type via & {:as more}]
              (merge {:type type, :f :pay
                      :value {:id 1, :kind :trampoline, :amount 1000000, :via via}}
                     more))
        r   (trampoline/analyze
              [(assoc-in (pay :ok ["n2"]) [:value :fee] 10000)
               (assoc-in (pay :ok ["n2" "n4"]) [:value :fee] 5000)
               (pay :fail ["n5"] :error "send_payment: Failed to build route")
               (pay :fail ["n5"] :error "send_payment: Failed to build route")
               (pay :fail ["n5"] :error [:new-invoice :receiver-down])
               (pay :info ["n6"])
               {:type :ok, :f :pay, :value {:id 9, :kind :invoice, :amount 5, :fee 1}}])]
    (is (true? (:valid? r)))
    (is (= 6 (:attempts r)))
    (is (= {:fail 3, :info 1, :ok 2} (:outcomes r)))
    (is (= {1 1, 2 1} (:hops r)))
    (is (= [["send_payment: Failed to build route" 2] ["[:new-invoice :receiver-down]" 1]] (:errors r)))
    (is (= {:min 0.5, :max 1.0} (:fee-percent r)))
    (testing "none succeeded: the kind wasn't exercised"
      (is (= :unknown (:valid? (trampoline/analyze [(pay :fail ["n5"] :error "no")]))))
      (is (= :unknown (:valid? (trampoline/analyze [])))))))
