(ns jepsen.fiber.swap
  "Cross-chain swaps through the hub (--cch, C-15), and their checker. The hub,
  its LND and the BTC user's LND are set up by jepsen.fiber.cch.

  A swap is one CCH order, in one of two directions:
    :send-btc     the BTC user (lnd-bob) makes a Lightning invoice, CCH's send_btc
                  turns it into a Fiber invoice of the hub node, and the CKB user
                  (n1) pays that in wrapped BTC; CCH then pays the Lightning
                  invoice from lnd-ingrid;
    :receive-btc  the CKB user makes a Fiber invoice, CCH's receive_btc turns it
                  into a Lightning hold invoice of lnd-ingrid, and the BTC user
                  pays that; CCH then pays the Fiber invoice from the hub node.
  Both payments of a swap carry one payment hash. The hub's incoming side is a
  hold invoice that CCH settles only with the preimage its outgoing payment
  brings back, and cancels if that payment fails. So every order should end
  completed (both sides paid) or refunded (neither paid); the plan's assertion
  is that orders end completed or refunded and both sides' funds agree.

  Swaps are drawn in advance from a stream of their own, :swap, like payments
  (jepsen.fiber.schedule): kind, amount, and the CKB user's preimage. The final
  :swap-read waits for every order to finish, then reads each swap's four legs
  (each side's payer and payee) and every balance involved."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [generator :as gen]]
            [jepsen.fiber [cch :as cch]
                          [checker :as fc]
                          [client :as client]
                          [rpc :as rpc]
                          [schedule :as schedule]])
  (:import (java.io IOException)
           (java.net ConnectException)
           (java.util SplittableRandom)
           (org.apache.http.conn ConnectTimeoutException)))

(def kinds
  #{:send-btc :receive-btc})

(defn parse-range
  "\"1000-20000\" -> [1000 20000]; \"5000\" -> [5000 5000]."
  [s]
  (let [[lo hi] (map parse-long (str/split s #"-" 2))]
    [lo (or hi lo)]))

(defn- hex32
  "32 random bytes from `r`, 0x-prefixed hex."
  [^SplittableRandom r]
  (apply str "0x" (repeatedly 4 #(format "%016x" (.nextLong r)))))

(defn ops
  "Endless swap ops drawn from `r`. Options: :swap-kinds, :swap-amount [min max]
  in sats. Every op draws a preimage, used by receive-btc only, so the kinds
  asked for don't shift the other draws."
  [^SplittableRandom r opts]
  (let [kinds   (vec (:swap-kinds opts [:send-btc :receive-btc]))
        [lo hi] (:swap-amount opts [1000 20000])]
    (repeatedly
      (fn []
        (let [id       (.nextLong r Long/MAX_VALUE)
              kind     (schedule/pick r kinds)
              amount   (+ lo (.nextLong r (inc (- hi lo))))
              preimage (hex32 r)]
          {:f     :swap
           :value (cond-> {:id id, :kind kind, :amount amount}
                    (= :receive-btc kind) (assoc :preimage preimage))})))))

(defn generator
  "Swaps for the fault window, about :swap-rate a second."
  [opts streams]
  (schedule/paced (schedule/gaps (schedule/gaps-stream streams :swap) (/ 1.0 (:swap-rate opts 0.5)))
                  (ops (schedule/stream streams :swap) opts)))

;; Running a swap

(def call-timeout-ms
  "How long a swap retries CCH's send_btc or receive_btc. CCH answers within a
  second, or reports a timeout and goes on creating the order; asking again
  with the same invoice returns that order, so retrying is safe."
  30000)

(def payment-timeout-ms
  "How long a swap waits for its payer's payment to finish. The payment is held
  until the hub's other side settles, so it takes as long as CCH is away."
  30000)

(defn- attempt
  "{::ok result}, or {::definite error} when the call certainly did nothing, or
  {::indefinite error} when it got no reply."
  [f]
  (try {::ok (f)}
       (catch ConnectException _ {::definite :down})
       (catch ConnectTimeoutException _ {::definite :unreachable})
       (catch clojure.lang.ExceptionInfo e
         (if (#{:rpc-error ::cch/http-error} (:type (ex-data e)))
           {::definite (ex-message e)}
           (throw e)))
       (catch IOException e
         {::indefinite [:no-reply (.getSimpleName (class e))]})))

(defn- poll
  "Calls (f) every 500 ms until (done? result) or `timeout-ms` pass; the last result."
  [timeout-ms done? f]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [res (f)]
        (if (or (done? res) (< deadline (System/currentTimeMillis)))
          res
          (do (Thread/sleep 500) (recur)))))))

(defn cch-call!
  "Calls CCH's `method`, retrying every second for call-timeout-ms. Returns
  {:ok order} or {:error last-error}, with :attempts. Any answer other than
  connection refused may have come from an order being created."
  [method params]
  (let [deadline (+ (System/currentTimeMillis) call-timeout-ms)]
    (loop [attempts 1, maybe? false]
      (let [res    (attempt #(rpc/call cch/cch-host method params {:timeout 10000}))
            maybe? (or maybe? (not (#{:down :unreachable} (::definite res))))]
        (cond (contains? res ::ok)
              {:ok (::ok res), :attempts attempts}

              (< deadline (System/currentTimeMillis))
              {:error (or (::definite res) (::indefinite res)), :attempts attempts, :maybe? maybe?}

              :else
              (do (Thread/sleep 1000)
                  (recur (inc attempts) maybe?)))))))

(defn- order-summary
  [order]
  {:order-status (:status order)
   :amount-sats  (rpc/hex->int (:amount_sats order))
   :fee-sats     (rpc/hex->int (:fee_sats order))})

(defn- final-order
  "The order once it is Success or Failed, waiting up to 5 s; nil if unreadable."
  [hash]
  (::ok (poll 5000 #(#{"Success" "Failed"} (get-in % [::ok :status]))
              #(attempt (fn [] (rpc/call cch/cch-host "get_cch_order" {:payment_hash hash}))))))

(defn- record!
  [test v]
  (swap! (:swaps test) conj (select-keys v [:id :kind :hash])))

(defn- no-order
  [op v res]
  (assoc op :type (if (:maybe? res) :info :fail), :value v, :error [:order (:error res)]))

(defn send-btc!
  [test op]
  (let [{:keys [id amount] :as v} (:value op)
        user (cch/ckb-user test)
        inv  (attempt #(cch/lnd-post cch/user-lnd "/v1/invoices"
                                     {:value (str amount), :memo (str "jepsen swap " id)}))]
    (if-not (contains? inv ::ok)
      (assoc op :type :fail, :value v, :error [:btc-invoice (or (::definite inv) (::indefinite inv))])
      (let [hash (cch/b64->hex (:r_hash (::ok inv)))
            v    (assoc v :hash hash)
            _    (record! test v)
            res  (cch-call! "send_btc" {:btc_pay_req (:payment_request (::ok inv))
                                        :currency    cch/currency})]
        (if-not (:ok res)
          (no-order op v res)
          (let [v   (merge v (order-summary (:ok res)))
                pay (attempt #(rpc/call user "send_payment"
                                        {:invoice (get-in res [:ok :incoming_invoice :Fiber])}
                                        {:timeout 10000}))]
            (cond
              (::definite pay)
              (assoc op :type :fail, :value v, :error [:ckb-payment (::definite pay)])

              (::indefinite pay)
              (assoc op :type :info, :value v, :error [:ckb-payment (::indefinite pay)])

              :else
              (let [final (poll payment-timeout-ms
                                #(client/terminal-statuses (get-in % [::ok :status]))
                                #(attempt (fn [] (rpc/call user "get_payment" {:payment_hash hash}))))
                    st    (get-in final [::ok :status])
                    v     (merge v {:ckb-payment st}
                                 (some-> (final-order hash) order-summary))]
                (case st
                  "Success" (assoc op :type :ok, :value v)
                  "Failed"  (assoc op :type :fail, :value v,
                                   :error [:refunded (get-in final [::ok :failed_error])])
                  (assoc op :type :info, :value v, :error [:not-finished st]))))))))))

(defn ln-pay!
  "The BTC user pays Lightning invoice `pay-req` through SendPaymentV2, waiting
  for the final status: the payment, or {::definite error}, or {::indefinite
  error} after payment-timeout-ms."
  [pay-req]
  (let [res (attempt #(cch/lnd-stream-last
                        (cch/lnd-post cch/user-lnd "/v2/router/send"
                                      {:payment_request     pay-req
                                       :timeout_seconds     20
                                       :fee_limit_sat       "1000"
                                       :no_inflight_updates true}
                                      {:timeout payment-timeout-ms, :raw? true})))]
    (cond (not (contains? res ::ok)) res
          (:error (::ok res))        {::definite (let [e (:error (::ok res))] (or (:message e) e))}
          :else                      (:result (::ok res)))))

(defn receive-btc!
  [test op]
  (let [{:keys [id amount preimage] :as v} (:value op)
        user (cch/ckb-user test)
        inv  (attempt #(rpc/call user "new_invoice"
                                 {:amount           (rpc/int->hex amount)
                                  :currency         cch/currency
                                  :description      (str "jepsen swap " id)
                                  :payment_preimage preimage
                                  :hash_algorithm   "sha256"
                                  :udt_type_script  (:script @(:swap-env test))}))]
    (if-not (contains? inv ::ok)
      (assoc op :type :fail, :value v, :error [:ckb-invoice (or (::definite inv) (::indefinite inv))])
      (let [hash (get-in inv [::ok :invoice :data :payment_hash])
            v    (assoc v :hash hash)
            _    (record! test v)
            res  (cch-call! "receive_btc" {:fiber_pay_req (:invoice_address (::ok inv))})]
        (if-not (:ok res)
          (no-order op v res)
          (let [v   (merge v (order-summary (:ok res)))
                pay (ln-pay! (get-in res [:ok :incoming_invoice :Lightning]))]
            (cond
              (::definite pay)
              (assoc op :type :fail, :value v, :error [:btc-payment (::definite pay)])

              (::indefinite pay)
              (assoc op :type :info, :value v, :error [:btc-payment (::indefinite pay)])

              :else
              (let [st (:status pay)
                    v  (merge v {:btc-payment st} (some-> (final-order hash) order-summary))]
                (case st
                  "SUCCEEDED" (assoc op :type :ok, :value v)
                  "FAILED"    (assoc op :type :fail, :value v, :error [:refunded (:failure_reason pay)])
                  (assoc op :type :info, :value v, :error [:not-finished st]))))))))))

(defmethod client/invoke-op :swap
  [test _node op]
  (case (get-in op [:value :kind])
    :send-btc    (send-btc! test op)
    :receive-btc (receive-btc! test op)))

;; The final read

(def legs
  "Each swap's four records: who pays and who is paid, on each side. For
  send-btc the CKB user pays the hub on Fiber and the hub pays the BTC user on
  Lightning; for receive-btc the other way round."
  [:ckb-payment :ckb-invoice :btc-payment :btc-invoice])

(def in-flight
  "Statuses of a payment or invoice whose money is still held."
  #{"Created" "Inflight" "Received" "INITIATED" "IN_FLIGHT" "ACCEPTED"})

(def unfinished-orders
  #{"IncomingAccepted" "OutgoingInFlight" "OutgoingSuccess"})

(defn- fiber-leg
  "A Fiber payment or invoice's status; :none when the node has no such record
  (an RPC error), :unknown when it couldn't be read."
  [node method hash]
  (let [r (attempt #(rpc/call node method {:payment_hash hash}))]
    (cond (contains? r ::ok)        (cond-> {:status (:status (::ok r))}
                                      (= "get_payment" method)
                                      (assoc :fee (or (rpc/hex->int (:fee (::ok r))) 0)))
          (string? (::definite r)) {:status :none, :error (::definite r)}
          :else                    {:status :unknown, :error (or (::definite r) (::indefinite r))})))

(defn- order-leg
  [hash]
  (let [r (attempt #(rpc/call cch/cch-host "get_cch_order" {:payment_hash hash}))]
    (cond (contains? r ::ok)       {:status (:status (::ok r))
                                    :amount (rpc/hex->int (:amount_sats (::ok r)))
                                    :fee    (rpc/hex->int (:fee_sats (::ok r)))}
          (string? (::definite r)) {:status :none, :error (::definite r)}
          :else                    {:status :unknown, :error (or (::definite r) (::indefinite r))})))

(defn- by-hash
  [k xs]
  (into {} (map (juxt k identity)) xs))

(defn- lnd-records
  "An LND's payments and invoices, by 0x-prefixed payment hash."
  [host]
  {:payments (by-hash #(str "0x" (:payment_hash %))
                      (:payments (cch/lnd-get host "/v1/payments" {:include_incomplete true
                                                                   :max_payments       100000})))
   :invoices (by-hash #(cch/b64->hex (:r_hash %))
                      (:invoices (cch/lnd-get host "/v1/invoices" {:num_max_invoices 100000})))})

(defn- ln-payment
  [records hash]
  (if-let [p (get-in records [:payments hash])]
    {:status (:status p), :value (cch/->long (:value_sat p)), :fee (cch/->long (:fee_sat p))}
    {:status :none}))

(defn- ln-invoice
  [records hash]
  (if-let [i (get-in records [:invoices hash])]
    {:status (:state i), :value (cch/->long (:value i)), :paid (cch/->long (:amt_paid_sat i))}
    {:status :none}))

(defn read-swaps
  "Every swap that got a payment hash, with its order and its four legs."
  [test]
  (let [{:keys [user hub]} @(:swap-env test)
        bob    (lnd-records cch/user-lnd)
        ingrid (lnd-records cch/hub-lnd)]
    (vec (for [{:keys [kind hash] :as s} @(:swaps test)]
           (let [send? (= :send-btc kind)]
             (assoc s
                    :order       (order-leg hash)
                    :ckb-payment (fiber-leg (if send? user hub) "get_payment" hash)
                    :ckb-invoice (fiber-leg (if send? hub user) "get_invoice" hash)
                    :btc-payment (ln-payment (if send? ingrid bob) hash)
                    :btc-invoice (ln-invoice (if send? bob ingrid) hash)))))))

(defn quiet?
  "Whether nothing about swap `s` can still change: its order is finished or
  never started, and no leg holds money or is unreadable."
  [s]
  (and (not (unfinished-orders (get-in s [:order :status])))
       (not-any? #(let [st (get-in s [% :status])] (or (in-flight st) (= :unknown st)))
                 (cons :order legs))))

(defn- balances
  [test]
  {:udt-channels (cch/udt-channels test)
   :ln-channels  {cch/user-lnd (cch/ln-channels cch/user-lnd)
                  cch/hub-lnd  (cch/ln-channels cch/hub-lnd)}})

(defn read!
  "Waits up to :swap-settle-timeout seconds for every swap to be quiet, reading
  every 3 s, then returns the last reading with the starting and final balances."
  [test]
  (let [t0       (System/currentTimeMillis)
        deadline (+ t0 (* 1000 (:swap-settle-timeout test 180)))]
    (loop [polls 1]
      (let [swaps (try (read-swaps test)
                       (catch Exception e (warn e "Couldn't read the swaps") nil))
            done? (or (and swaps (every? quiet? swaps))
                      (< deadline (System/currentTimeMillis)))]
        (if-not done?
          (do (Thread/sleep 3000)
              (recur (inc polls)))
          {:swaps     swaps
           :polls     polls
           :waited-ms (- (System/currentTimeMillis) t0)
           :initial   (select-keys @(:swap-env test) [:user :hub :udt-channels :ln-channels])
           :final     (try (balances test)
                           (catch Exception e (warn e "Couldn't read the balances") nil))})))))

(defmethod client/invoke-op :swap-read
  [test _node op]
  (assoc op :type :ok, :value (read! test)))

;; Checking

(defn paid
  "Which sides of swap `s` paid out, by the payee's and by the payer's record."
  [s]
  {:btc         (= "SETTLED" (get-in s [:btc-invoice :status]))
   :btc-payment (= "SUCCEEDED" (get-in s [:btc-payment :status]))
   :ckb         (= "Paid" (get-in s [:ckb-invoice :status]))
   :ckb-payment (= "Success" (get-in s [:ckb-payment :status]))})

(defn outcome
  "How swap `s` ended: :completed, :refunded (order failed, nothing paid),
  :unpaid (order pending or never made, nothing paid), :unknown (a record
  couldn't be read), or :broken."
  [s problems]
  (let [{:keys [btc ckb]} (paid s)
        order             (get-in s [:order :status])]
    (cond (some #(= :unknown (get-in s [% :status])) (cons :order legs)) :unknown
          (seq problems)                                               :broken
          (and btc ckb (= "Success" order))                            :completed
          (= "Failed" order)                                           :refunded
          :else                                                        :unpaid)))

(defn swap-problems
  "What is wrong with swap `s`, whose op ended with type `client` (:ok, :fail,
  :info or nil)."
  [s client]
  (let [{:keys [btc btc-payment ckb ckb-payment]} (paid s)
        order  (get-in s [:order :status])
        held   (into {} (for [k legs
                              :let [st (get-in s [k :status])]
                              :when (in-flight st)]
                          [k st]))
        ; Who is out of pocket when one side paid and the other didn't: the
        ; hub pays out on the outgoing side, the user on the incoming one.
        out    (if (= :send-btc (:kind s)) btc ckb)
        in     (if (= :send-btc (:kind s)) ckb btc)]
    (cond-> []
      (unfinished-orders order)
      (conj [:order-unfinished order])

      (seq held)
      (conj [:held held])

      (not= btc btc-payment)
      (conj [:btc-records-disagree (select-keys s [:btc-payment :btc-invoice])])

      (not= ckb ckb-payment)
      (conj [:ckb-records-disagree (select-keys s [:ckb-payment :ckb-invoice])])

      (not= in out)
      (conj [:one-sided {:loser (if out :hub :user), :btc-paid btc, :ckb-paid ckb}])

      (and (= "Success" order) (not (and btc ckb)))
      (conj [:success-unpaid order])

      (and (not= "Success" order) (or btc ckb))
      (conj [:paid-but-order order])

      (and (= :ok client) (not (and btc ckb)))
      (conj [:client-saw-success])

      (and (= :fail client) (or btc ckb))
      (conj [:client-saw-failure]))))

(defn- sum-local
  "The sum of `k` over the channels in `chs` whose `id-k` is in `ids`."
  [chs ids id-k k]
  (reduce + 0 (for [ch chs :when (ids (id-k ch))] (or (k ch) 0))))

(defn balance-check
  "Compares the CKB user's and the BTC user's balance changes with what the
  paid sides of the swaps imply. A swap's order has the amount its payer pays,
  fee included; its payee gets that less the hub's fee."
  [read swaps]
  (let [{:keys [initial final]} read
        user    (:user initial)
        amount  #(get-in % [:order :amount])
        fee     #(get-in % [:order :fee])
        sides   (map (juxt identity paid) swaps)
        missing (vec (for [[s p] sides
                           :when (and (or (:btc p) (:ckb p)) (not (and (amount s) (fee s))))]
                       (:hash s)))
        ; What a paid side moves: the payer pays the order amount plus its own
        ; routing fee; the payee gets the order amount less the hub's fee.
        expect  (fn [side kind fee-leg]
                  (reduce + 0 (for [[s p] sides :when (side p)]
                                (if (= kind (:kind s))
                                  (- (amount s) (fee s))
                                  (- (+ (amount s) (or (get-in s [fee-leg :fee]) 0)))))))
        udt-ids (set (map :channel_id (get-in initial [:udt-channels user])))
        ln-ids  (set (map :chan-id (get-in initial [:ln-channels cch/user-lnd])))
        ckb     (fn [m] (sum-local (get-in m [:udt-channels user]) udt-ids :channel_id
                                   #(fc/amount % :local_balance)))
        bob     (fn [m] (sum-local (get-in m [:ln-channels cch/user-lnd]) ln-ids :chan-id
                                   :local-balance))]
    (if (or (seq missing) (nil? final))
      {:valid? :unknown, :missing-orders missing}
      (let [exp-bob   (expect :btc :send-btc :btc-payment)
            exp-ckb   (expect :ckb :receive-btc :ckb-payment)
            ckb-delta (- (ckb final) (ckb initial))
            bob-delta (- (bob final) (bob initial))]
        {:valid?   (and (= exp-ckb ckb-delta) (= exp-bob bob-delta))
         :ckb-user {:delta ckb-delta, :expected exp-ckb}
         :btc-user {:delta bob-delta, :expected exp-bob}}))))

(defn ln-channel-problems
  "Lightning channels between the two LNDs that are inactive, still hold HTLCs,
  or whose ends disagree on the balances."
  [final]
  (let [bob    (get-in final [:ln-channels cch/user-lnd])
        ingrid (by-hash :chan-id (get-in final [:ln-channels cch/hub-lnd]))]
    (vec (for [ch   bob
               :let [peer     (ingrid (:chan-id ch))
                     problems (cond-> []
                                (nil? peer)                (conj :missing-at-hub)
                                (not (:active ch))         (conj :inactive)
                                (pos? (:pending-htlcs ch)) (conj [:pending-htlcs (:pending-htlcs ch)])
                                (and peer (or (not= (:local-balance ch) (:remote-balance peer))
                                              (not= (:remote-balance ch) (:local-balance peer))))
                                (conj [:mirror {:bob ch, :hub peer}]))]
               :when (seq problems)]
           {:chan-id (:chan-id ch), :problems problems}))))

(defn checker
  "Valid when every swap ended completed, refunded or unpaid, with both
  sides' records agreeing, no money held and the order's status matching; when
  the CKB user's and the BTC user's balances moved by what the paid swaps
  imply; and when the wrapped-BTC and Lightning channels are ready, hold
  nothing and agree at both ends. Unknown when a record couldn't be read, and
  when no swap completed, which means the swaps didn't run."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [read (->> history
                      (filter #(and (= :ok (:type %)) (= :swap-read (:f %))))
                      last
                      :value)]
        (if-not (and read (:swaps read))
          {:valid? :unknown, :error "no final swap read"}
          (let [client   (into {} (for [op    history
                                        :when (and (= :swap (:f op))
                                                   (#{:ok :fail :info} (:type op))
                                                   (get-in op [:value :hash]))]
                                    [(get-in op [:value :hash]) (:type op)]))
                swaps    (:swaps read)
                judged   (for [s swaps]
                           (let [p (swap-problems s (client (:hash s)))]
                             (assoc s :problems p :outcome (outcome s p))))
                outcomes (frequencies (map :outcome judged))
                broken   (vec (for [s judged :when (seq (:problems s))]
                                (select-keys s [:id :kind :hash :order :problems
                                                :ckb-payment :ckb-invoice :btc-payment
                                                :btc-invoice])))
                balance  (balance-check read swaps)
                udt      (when (:final read)
                           (fc/channel-problems (get-in read [:initial :udt-channels])
                                                (get-in read [:final :udt-channels])))
                ln       (some-> (:final read) ln-channel-problems)
                valid    (cond (or (pos? (outcomes :unknown 0)) (nil? (:final read))
                                   (= :unknown (:valid? balance)))
                               :unknown

                               (or (seq broken) (false? (:valid? balance)) (seq udt) (seq ln))
                               false

                               (zero? (outcomes :completed 0))
                               :unknown

                               :else
                               true)]
            (cond-> {:valid?               valid
                     :outcomes             outcomes
                     :broken               broken
                     :balances             balance
                     :udt-channel-problems udt
                     :ln-channel-problems  ln
                     :read-polls           (:polls read)
                     :read-waited-ms       (:waited-ms read)}
              (and (not (false? valid)) (zero? (outcomes :completed 0)))
              (assoc :error "no swap completed"))))))))

(defn check-opts!
  "Throws unless the swap faults come with --cch."
  [opts]
  (let [needs (filter #{:cch-kill :lnd-disconnect} (:faults opts))]
    (when (and (seq needs) (not (:cch opts)))
      (throw (IllegalArgumentException.
               (str "--faults " (str/join "," (map name needs)) " needs --cch"))))))

(defn final-generator
  "The final swap read, with --cch."
  [opts]
  (when (:cch opts)
    (gen/clients (gen/once {:f :swap-read}))))
