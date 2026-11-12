(ns jepsen.fiber.hold-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gt]
            [jepsen.fiber :as fiber]
            [jepsen.fiber [client :as client]
                          [hold :as hold]
                          [invoice :as invoice]
                          [rpc :as rpc]
                          [schedule :as schedule]])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetSocketAddress)))

(def nodes ["n1" "n2" "n3"])

(deftest payment-hash
  (testing "SHA-256 of the preimage's 32 bytes"
    (is (= "0x66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925"
           (hold/payment-hash (str "0x" (apply str (repeat 64 "0"))))))
    (is (= "0xaf9613760f72635fbdb44a5a0a63c39f12af30f950a6ee5c971be188e89c4051"
           (hold/payment-hash (str "0x" (apply str (repeat 32 "ff"))))))))

(deftest plans
  (let [plans (map #(hold/plan {} %) (range 1000))]
    (testing "from the attempt id alone"
      (is (= plans (map #(hold/plan {} %) (range 1000))))
      (is (= (hold/plan {} 7) (hold/plan {:seed 99} 7))))
    (testing "held 1-5 s, about one in four cancelled"
      (is (every? #(<= 1000 (:hold-ms %) 5000) plans))
      (is (< 2 (count (set (map #(quot (:hold-ms %) 1000) plans)))))
      (is (< 200 (count (filter #(= :cancel (:action %)) plans)) 300))))
  (testing "--hold-time and --hold-cancel"
    (let [plans (map #(hold/plan {:hold-time [20 30], :hold-cancel 100} %) (range 200))]
      (is (every? #(<= 20000 (:hold-ms %) 30000) plans))
      (is (every? #(= :cancel (:action %)) plans)))
    (is (every? #(= {:action :settle, :hold-ms 0} %)
                (map #(hold/plan {:hold-time [0 0], :hold-cancel 0} %) (range 200))))))

(deftest the-fault-window-keeps-its-payments
  (testing "hold in the mix changes the kinds drawn, not ids, amounts or destinations"
    (let [opts (fn [kinds] {:nodes nodes, :rate 5, :time-limit 60, :nemesis-interval 10
                            :kill-targets [:one], :faults [:kill], :pay-kinds kinds})
          run  (fn [o] (->> (gt/simulate (gen/context {:concurrency 3})
                                         (fiber/fault-window o (schedule/streams 9))
                                         (fn [_ op] (-> op (assoc :type :ok) (update :time + 1000000))))
                            (filter #(#{:invoke :info} (:type %)))
                            (map (juxt :f #(if (= :pay (:f %)) (dissoc (:value %) :kind) (:value %))))))
          a    (run (opts [:keysend :invoice]))
          b    (run (opts [:keysend :hold]))]
      (is (< 250 (count a)))
      (is (= a b)))))

(defn- fake-fnn
  "An HTTP server answering fnn's JSON-RPC calls with `answer` (fn [node method
  params] -> result, or {::error message} for a JSON-RPC error), recording each
  call in `calls` as [node method params]. The node is the request's path."
  [calls answer]
  (doto (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)
    (.createContext "/" (reify HttpHandler
                          (handle [_ ex]
                            (let [^HttpExchange ex ex
                                  node (subs (.getPath (.getRequestURI ex)) 1)
                                  req  (json/parse-string (slurp (.getRequestBody ex)) true)
                                  m    (:method req)
                                  p    (first (:params req))
                                  _    (swap! calls conj [node m p])
                                  res  (answer node m p)
                                  body (.getBytes (json/generate-string
                                                    (if-let [e (::error res)]
                                                      {:jsonrpc "2.0", :id 1, :error {:code -32000, :message e}}
                                                      {:jsonrpc "2.0", :id 1, :result res})))]
                              (.sendResponseHeaders ex 200 (count body))
                              (with-open [o (.getResponseBody ex)] (.write o body))))))
    (.start)))

(defmacro with-fnn
  "Runs `body` with every node's RPC answered by a fake fnn (see fake-fnn),
  except n2, which is down."
  [calls answer & body]
  `(let [server# (fake-fnn ~calls ~answer)
         port#   (.getPort (.getAddress server#))]
     (try
       (with-redefs [rpc/node-url (fn [node#]
                                    (if (= "n2" node#)
                                      "http://127.0.0.1:1"
                                      (str "http://127.0.0.1:" port# "/" node#)))]
         ~@body)
       (finally (.stop server# 0)))))

(defn- test-map
  [& {:as more}]
  (merge {:nodes nodes, :pubkeys (atom {}), :payments (atom []), :holds (atom {}), :ledger true
          :hold-time [0 0], :hold-cancel 0}
         more))

(def op {:type :invoke, :f :pay, :value {:id 7, :amount 2000000, :to "n3", :kind :hold}})

(def pre (invoice/preimage 7))

(def h (hold/payment-hash pre))

(defn- methods-called
  [calls]
  (map (fn [[node m _]] [node m]) calls))

(deftest holds-then-settles
  (let [calls   (atom [])
        settled (atom false)
        polls   (atom 0)]
    (with-fnn calls
      (fn [_ m _]
        (case m
          "new_invoice"    {:invoice_address "fibd1hold", :invoice {:data {:payment_hash h}}}
          "send_payment"   {:payment_hash h, :status "Created"}
          ; Open at first, then the TLC arrives.
          "get_invoice"    {:status (cond @settled "Paid", (< 1 (swap! polls inc)) "Received", :else "Open")}
          "get_payment"    {:payment_hash h, :status (if @settled "Success" "Inflight"), :fee "0x3e8"}
          "settle_invoice" (do (reset! settled true) {})))
      (let [test (test-map)
            done (client/pay! test "n1" op)
            call (fn [m] (some (fn [[_ m' p]] (when (= m m') p)) @calls))]
        (is (= :ok (:type done)))
        (testing "the receiver gets the payment hash and no preimage"
          (is (= h (:payment_hash (call "new_invoice"))))
          (is (= "sha256" (:hash_algorithm (call "new_invoice"))))
          (is (not (contains? (call "new_invoice") :payment_preimage))))
        (testing "the payment carries its attempt id, for --ledger"
          (is (= {:invoice "fibd1hold", :custom_records {(keyword "0xf1be") "0x0000000000000007"}}
                 (call "send_payment"))))
        (testing "the preimage goes to the receiver once it holds the payment, and not before"
          (is (= {:payment_hash h, :payment_preimage pre} (call "settle_invoice")))
          (let [ms (methods-called @calls)]
            (is (= ["n3" "settle_invoice"] (first (filter (comp #{"settle_invoice"} second) ms))))
            (is (< 1 (count (take-while #(not= "settle_invoice" (second %))
                                        (drop-while #(not= "get_invoice" (second %)) ms)))))))
        (testing "the op says what happened"
          (is (= {:action :settle, :hold-ms 0, :result :settled}
                 (dissoc (get-in done [:value :hold]) :received-ms)))
          (is (<= 0 (get-in done [:value :hold :received-ms])))
          (is (= h (get-in done [:value :hash])))
          (is (= 1000 (get-in done [:value :fee]))))
        (testing "and the invoice is remembered as held and released"
          (is (= {:id 7, :from "n1", :to "n3", :amount 2000000, :hash h, :preimage pre
                  :action :settle, :hold-ms 0, :held? true, :released? true, :result :settled}
                 (get @(:holds test) 7))))))))

(deftest holds-then-cancels
  (let [calls     (atom [])
        cancelled (atom false)]
    (with-fnn calls
      (fn [_ m _]
        (case m
          "new_invoice"    {:invoice_address "fibd1hold"}
          "send_payment"   {:payment_hash h, :status "Created"}
          "get_invoice"    {:status (if @cancelled "Cancelled" "Received")}
          "get_payment"    (if @cancelled
                             {:status "Failed", :failed_error "InvoiceCancelled"}
                             {:status "Inflight"})
          "cancel_invoice" (do (reset! cancelled true) {:status "Cancelled"})))
      (let [test (test-map :hold-cancel 100)
            done (client/pay! test "n1" op)]
        (is (= :fail (:type done)))
        (is (= "InvoiceCancelled" (:error done)))
        (is (= :cancelled (get-in done [:value :hold :result])))
        (is (some #{["n3" "cancel_invoice"]} (methods-called @calls)))
        (testing "the receiver never gets the preimage"
          (is (not-any? #{"settle_invoice"} (map second @calls)))
          (is (not (:released? (get @(:holds test) 7)))))))))

(deftest nothing-to-hold
  (testing "the payment fails before it reaches the receiver"
    (let [calls (atom [])]
      (with-fnn calls
        (fn [_ m _]
          (case m
            "new_invoice"  {:invoice_address "fibd1hold"}
            "send_payment" {:payment_hash h, :status "Created"}
            "get_invoice"  {:status "Open"}
            "get_payment"  {:status "Failed", :failed_error "no path found"}))
        (let [test (test-map)
              done (client/pay! test "n1" op)]
          (is (= :fail (:type done)))
          (is (= {:action :settle, :hold-ms 0, :not-held {:invoice "Open", :payment "Failed"}}
                 (get-in done [:value :hold])))
          (is (not-any? #{"settle_invoice" "cancel_invoice"} (map second @calls)))
          (testing "the invoice stays on the list for :resolve-holds"
            (is (nil? (:result (get @(:holds test) 7)))))))))
  (testing "send_payment refuses"
    (let [calls (atom [])]
      (with-fnn calls
        (fn [_ m _]
          (case m
            "new_invoice"  {:invoice_address "fibd1hold"}
            "send_payment" {::error "Failed to build route"}))
        (let [done (client/pay! (test-map) "n1" op)]
          (is (= :fail (:type done)))
          (is (= {:action :settle, :hold-ms 0, :not-held :not-sent} (get-in done [:value :hold])))))))
  (testing "a receiver that is down: no invoice, no payment"
    (let [calls (atom [])]
      (with-fnn calls (fn [_ _ _] {})
        (let [test (test-map)
              done (client/pay! test "n1" (assoc-in op [:value :to] "n2"))]
          (is (= :fail (:type done)))
          (is (= [:new-invoice :receiver-down] (:error done)))
          (is (empty? @calls))
          (is (empty? @(:holds test))))))))

(deftest success-before-the-settle
  (let [calls (atom [])]
    (with-fnn calls
      (fn [_ m _]
        (case m
          "new_invoice"  {:invoice_address "fibd1hold"}
          "send_payment" {:payment_hash h, :status "Created"}
          "get_invoice"  {:status "Received"}
          "get_payment"  {:status "Success", :fee "0x0"}))
      (let [test (test-map)
            done (client/pay! test "n1" op)]
        (is (true? (get-in done [:value :hold :early-success])))
        (is (true? (:early-success (get @(:holds test) 7))))
        (testing "the preimage stays with the harness"
          (is (not-any? #{"settle_invoice"} (map second @calls))))))))

(defn- rec
  [id to action & {:as more}]
  (merge {:id id, :from "n1", :to to, :amount 5, :hash (str "0x" id), :preimage (str "0xp" id)
          :action action, :hold-ms 0}
         more))

(deftest resolves
  (let [calls  (atom [])
        status {"0x1" "Received", "0x2" "Received", "0x3" "Open", "0x4" "Paid", "0x6" "Received"}
        test   (test-map :holds (atom (into {} (map (juxt :id identity))
                                            [(rec 1 "n3" :settle)
                                             (rec 2 "n3" :cancel, :result [:cancel :receiver-down])
                                             (rec 3 "n1" :settle)
                                             (rec 4 "n1" :settle, :result [:settle [:no-reply "SocketTimeoutException"]]
                                                  :released? true)
                                             ; Over: not asked about again.
                                             (rec 5 "n3" :settle, :result :settled, :released? true)
                                             (rec 6 "n3" :settle)
                                             (rec 8 "n2" :settle)])))]
    (with-redefs [hold/reach-timeout-ms 100]
      (with-fnn calls
        (fn [_ m p]
          (case m
            "node_info"      {}
            "get_invoice"    {:status (status (:payment_hash p))}
            "settle_invoice" (if (= "0x6" (:payment_hash p)) {::error "Hash mismatch"} {})
            "cancel_invoice" {:status "Cancelled"}))
        (let [done (client/invoke-op test "n1" {:type :invoke, :f :resolve-holds})
              by   (fn [m] (set (for [[_ m' p] @calls :when (= m m')] (:payment_hash p))))]
          (is (= :ok (:type done)))
          (testing "a held payment gets its plan's call, an invoice nothing reached is cancelled"
            (is (= #{"0x1" "0x6"} (by "settle_invoice")))
            (is (= #{"0x2" "0x3"} (by "cancel_invoice")))
            (is (= #{"0x1" "0x2" "0x3" "0x4" "0x6"} (by "get_invoice"))))
          (testing "what was done, by receiver; a receiver that is down is waited for once"
            (is (= {"n1" {:cancelled 1, :over 1}
                    "n2" {:unreachable 1}
                    "n3" {:settled 1, :cancelled 1, :settle 1}}
                   (:value done))))
          (testing "and remembered"
            (let [holds @(:holds test)]
              (is (= [:settled :cancelled :cancelled] (map #(:result (holds %)) [1 2 3])))
              (is (true? (:released? (holds 1))))
              (is (not (:released? (holds 3))))
              (is (= [:settle "settle_invoice: Hash mismatch"] (:result (holds 6))))
              (testing "a refused settle releases nothing"
                (is (not (:released? (holds 6))))))))))))

(deftest reads-both-ends
  (let [calls (atom [])
        test  (test-map :holds (atom {7 (rec 7 "n3" :settle, :result :settled, :released? true, :held? true)}))]
    (with-fnn calls
      (fn [_ m _]
        (case m
          "get_invoice" {:status "Paid"}
          "get_payment" {:status "Success", :fee "0x10"}))
      (let [done (client/invoke-op test "n1" {:type :invoke, :f :read-holds})]
        (is (= :ok (:type done)))
        (is (= [{:id 7, :from "n1", :to "n3", :amount 5, :hash "0x7", :action :settle, :hold-ms 0
                 :result :settled, :released? true, :held? true
                 :invoice-status "Paid", :payment-status "Success", :fee 16}]
               (:value done)))
        (is (= #{["n3" "get_invoice"] ["n1" "get_payment"]} (set (methods-called @calls))))))))

(deftest outcomes
  (let [o #(hold/outcome (merge {:invoice-status %1, :payment-status %2} %3))]
    (is (= :paid (o "Paid" "Success" {:released? true})))
    (is (= :unpaid (o "Cancelled" "Failed" nil)))
    (is (= :unpaid (o "Open" nil {:no-payment true})))
    (is (= :unpaid (o "Cancelled" "Failed" {:released? true})))
    (is (= :held (o "Received" "Inflight" {:released? true})))
    (is (= :held (o "Received" "Failed" nil)))
    (is (= :unfinished (o "Paid" "Inflight" {:released? true})))
    (is (= :unfinished (o "Cancelled" "Inflight" nil)))
    (is (= :unread (o nil "Success" {:released? true})))
    (is (= :unread (o "Paid" nil {:released? true, :payment-unread "Connection refused"})))
    (testing "the ends disagree"
      (is (= :paid-but-failed (o "Paid" "Failed" {:released? true})))
      (is (= :success-but-unpaid (o "Cancelled" "Success" {:released? true}))))
    (testing "money moved without the preimage"
      (is (= :success-without-preimage (o "Cancelled" "Success" nil)))
      (is (= :success-without-preimage (o "Paid" "Success" nil)))
      (is (= :paid-without-preimage (o "Paid" "Failed" nil)))
      (is (= :paid-without-preimage (o "Paid" "Inflight" nil)))
      (is (= :success-before-settle (o "Paid" "Success" {:released? true, :early-success true}))))))

(defn- row
  [id action invoice payment & {:as more}]
  (merge {:id id, :from "n1", :to "n3", :amount 5, :hash (str "0x" id), :action action, :hold-ms 0
          :invoice-status invoice, :payment-status payment}
         more))

(deftest checker
  (let [pay  (fn [type id] {:type type, :f :pay, :value {:id id, :kind :hold, :amount 5}})
        base [(pay :ok 1) (pay :fail 2) (pay :fail 3) (pay :info 4)
              {:type :fail, :f :pay, :value {:id 5, :kind :hold}, :error [:new-invoice :receiver-down]}
              {:type :ok, :f :pay, :value {:id 6, :amount 5}}]
        read (fn [rows] [{:type :ok, :f :read-holds, :value rows}])
        good [(row 1 :settle "Paid" "Success", :held? true, :released? true, :result :settled)
              (row 2 :cancel "Cancelled" "Failed", :held? true, :result :cancelled)
              (row 3 :settle "Cancelled" "Failed", :result :cancelled)
              (row 4 :settle "Paid" "Success", :held? true, :released? true, :result :settled)]]
    (testing "settled ones paid, cancelled ones not"
      (let [r (hold/analyze (concat base (read good)))]
        (is (true? (:valid? r)))
        (is (= 5 (:attempts r)))
        (is (= 4 (:invoices r)))
        (is (= 1 (:no-invoice r)))
        (is (= 3 (:held r)))
        (is (= {:paid 2, :unpaid 2} (:outcomes r)))
        (is (= {:settle {:paid 2}, :cancel {:unpaid 1}} (:by-action r)))
        (is (empty? (:violations r)))))
    (testing "a cancelled payment that succeeded"
      (let [r (hold/analyze (concat base (read (assoc good 1 (row 2 :cancel "Cancelled" "Success"
                                                                  :held? true, :result :cancelled)))))]
        (is (false? (:valid? r)))
        (is (= [[2 :success-without-preimage]] (map (juxt :id :outcome) (:violations r))))))
    (testing "a payment still held at the end"
      (let [r (hold/analyze (concat base (read (assoc good 0 (row 1 :settle "Received" "Inflight"
                                                                  :held? true, :released? true
                                                                  :result :settled)))))]
        (is (= :unknown (:valid? r)))
        (is (= [[1 :held]] (map (juxt :id :outcome) (:not-final r))))))
    (testing "nothing was ever held, no read, nothing to read"
      (is (= :unknown (:valid? (hold/analyze (concat base (read [(row 3 :settle "Cancelled" "Failed")]))))))
      (is (= :unknown (:valid? (hold/analyze base))))
      (is (= :unknown (:valid? (hold/analyze (read []))))))))

(deftest generator
  (let [opts {:nodes nodes, :rate 5, :time-limit 30, :quiesce 30, :nemesis-interval 10
              :kill-targets [:one], :faults [:kill], :seed 20261112}
        ops  (fn [o] (->> (gt/simulate (gen/context {:concurrency 3})
                                       (:generator (fiber/fiber-test o))
                                       (fn [_ctx op] (-> op (assoc :type :ok) (update :time + 50000000))))
                          (filter #(#{:invoke :info :sleep} (:type %)))
                          (map #(if (= :sleep (:type %)) :sleep (:f %)))))
        tail (fn [o] (drop-while #{:pay :kill :start} (ops o)))]
    (testing "without the kind, no hold ops"
      (is (not-any? #{:resolve-holds :read-holds} (ops opts))))
    (testing "the calls the faults prevented come after the healing and before the wait; the read is last"
      (let [t (tail (assoc opts :pay-kinds [:hold :invoice]))]
        (is (= [:resolve-holds :sleep] (take 2 (drop-while (complement #{:resolve-holds}) t))))
        (is (= [:read :read-holds] (take-last 2 t)))
        (is (= 1 (count (filter #{:resolve-holds} t))))))
    (testing "with a recovery window, again after it"
      (let [t (tail (assoc opts :pay-kinds [:hold :invoice] :availability-window 10))]
        (is (= 2 (count (filter #{:resolve-holds} t))))
        (is (= :resolve-holds (last (take-while (complement #{:probe}) t))))))))
