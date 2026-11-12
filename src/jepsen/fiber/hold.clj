(ns jepsen.fiber.hold
  "Hold-invoice payments (--pay-kinds hold). The receiver creates an invoice
  from a payment hash alone (new_invoice with payment_hash and no preimage), so
  it cannot settle what arrives: it holds the payment's TLC, the invoice turns
  Received, and the payment stays in flight on every hop until someone gives
  the receiver the preimage (settle_invoice) or tells it to give the payment up
  (cancel_invoice). Here the harness is that someone: it keeps the preimage,
  derived from the attempt id, and the node never learns it before the settle.

  Each payment has a plan drawn from its attempt id alone (`plan`): how long
  the receiver holds it (--hold-time) and whether it then settles or cancels
  (--hold-cancel percent cancel). The client that sent the payment waits until
  the invoice is Received, sleeps the hold time, and makes the receiver's call.
  So TLCs stay pending for seconds while faults go on around them, where other
  kinds' TLCs last milliseconds, and a fault can fall between the receiver's
  taking the payment and its settling.

  A call that a fault kept from the receiver is made up for later: after the
  faults heal, :resolve-holds visits every hold invoice not known to be over
  and does what its plan says if the receiver holds the payment, or cancels it
  if nothing has arrived, so that a payment still on its way fails at once
  rather than being held with nobody to settle it. Fiber itself keeps a held
  payment until its TLC expires, hours later.

  After the final read, :read-holds reads every hold invoice's status on its
  receiver and its payment's on its sender, and the checker wants:
    - Success at the sender only with the invoice Paid at the receiver, and
      Paid only with Success;
    - neither without the preimage: a payment that is Success, or an invoice
      that is Paid, though the harness never gave the receiver the preimage
      (the plan was to cancel, or the settle never happened), is money that
      moved on a secret nobody had;
    - no Success before the settle: a sender that reports Success while the
      receiver still holds the payment.
  Whether the amounts add up is what --ledger checks, as for every kind."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [generator :as gen]]
            [jepsen.util :refer [await-fn]]
            [jepsen.fiber [client :as client]
                          [invoice :as invoice]
                          [mpp :as mpp]
                          [rpc :as rpc]])
  (:import (java.io IOException)
           (java.net ConnectException)
           (java.security MessageDigest)
           (java.util SplittableRandom)
           (org.apache.http.conn ConnectTimeoutException)))

(def default-hold-time-s
  "--hold-time's default, seconds: long enough for a fault to fall inside a
  hold, short enough that the client, which waits, still sends payments."
  [1 5])

(def default-cancel-percent
  "--hold-cancel's default: one held payment in four is cancelled."
  25)

(def received-timeout-ms
  "How long the client waits for the receiver to hold a payment before it leaves
  the invoice to :resolve-holds."
  15000)

(def reach-timeout-ms
  "How long :resolve-holds waits for a receiver to answer after the faults heal."
  60000)

(def poll-ms 200)

(defn enabled?
  "Whether the test sends hold-invoice payments."
  [opts]
  (boolean (some #{:hold} (:pay-kinds opts))))

(defn payment-hash
  "The SHA-256 of `preimage` (32 bytes as 0x-prefixed hex), as 0x-prefixed hex:
  the payment hash of an invoice whose hash_algorithm is sha256. Fiber's
  default algorithm is CKB's Blake2b, which the JDK doesn't have."
  [^String preimage]
  (let [hex   (subs preimage 2)
        bytes (byte-array (for [i (range 0 (count hex) 2)]
                            (unchecked-byte (Integer/parseInt (subs hex i (+ i 2)) 16))))]
    (str "0x" (apply str (map #(format "%02x" %)
                              (.digest (MessageDigest/getInstance "SHA-256") bytes))))))

(defn plan
  "What the receiver does with the payment of attempt `id` once it holds it:
  {:action :settle or :cancel, :hold-ms how long it holds it first}. Drawn from
  the id alone, not from a stream of the seed, so the hold kind draws nothing
  that other payments' values depend on, and a seed gives the same plans again."
  [test id]
  (let [r       (SplittableRandom. (long (or id 0)))
        [lo hi] (:hold-time test default-hold-time-s)
        roll    (.nextInt r 100)
        ms      (+ (* 1000 lo) (.nextInt r (inc (* 1000 (- hi lo)))))]
    {:action  (if (< roll (:hold-cancel test default-cancel-percent)) :cancel :settle)
     :hold-ms ms}))

(defn record!
  "Remembers a hold invoice, by its attempt id, for :resolve-holds and
  :read-holds. Returns it."
  [test rec]
  (when-let [holds (:holds test)]
    (swap! holds assoc (:id rec) rec))
  rec)

(defn note!
  "Adds `m` to what is remembered of the hold invoice of attempt `id`."
  [test id m]
  (when-let [holds (:holds test)]
    (swap! holds update id merge m)))

(defn- attempt
  "{:ok (f)}, or how the call failed:
    {:error e}                    the node certainly didn't act on it: it wasn't
                                  reached, or it refused;
    {:error e, :indefinite? true} it may have: no reply came."
  [f]
  (try {:ok (f)}
       (catch ConnectException _ {:error :receiver-down})
       (catch ConnectTimeoutException _ {:error :receiver-unreachable})
       (catch clojure.lang.ExceptionInfo e
         (if (= :rpc-error (:type (ex-data e)))
           {:error (ex-message e)}
           (throw e)))
       (catch IOException e
         {:error [:no-reply (.getSimpleName (class e))], :indefinite? true})))

(defn settle!
  "Gives the receiver the preimage of hold invoice `rec` (settle_invoice), which
  it accepts only while the invoice is Received. Returns :settled, or
  [:settle error]. The invoice is marked :released? when the receiver has, or
  may have, the preimage: it accepted the call, or no reply came."
  [test {:keys [id to hash preimage]}]
  (let [r      (attempt #(rpc/call to "settle_invoice" {:payment_hash hash, :payment_preimage preimage}))
        result (if (contains? r :ok) :settled [:settle (:error r)])]
    (note! test id (cond-> {:result result}
                     (or (contains? r :ok) (:indefinite? r)) (assoc :released? true)))
    result))

(defn cancel!
  "Tells the receiver to give up hold invoice `rec` (cancel_invoice): it fails
  the payment it holds, and any that arrives later. Returns :cancelled, or
  [:cancel error]."
  [test {:keys [id to hash]}]
  (let [r      (attempt #(rpc/call to "cancel_invoice" {:payment_hash hash}))
        result (if (contains? r :ok) :cancelled [:cancel (:error r)])]
    (note! test id {:result result})
    result))

(defn act!
  "The receiver's call that the plan of hold invoice `rec` asks for."
  [test rec]
  (case (:action rec)
    :settle (settle! test rec)
    :cancel (cancel! test rec)))

(defn- status
  "The :status `method` gives for payment hash `hash` on `node`, or nil."
  [node method hash]
  (try (:status (rpc/call node method {:payment_hash hash}))
       (catch Exception _ nil)))

(defn await-received
  "Polls until the receiver `to` holds the payment (its invoice is Received),
  the invoice or the payment of sender `from` is over, or received-timeout-ms
  passes. Returns {:invoice status, :payment status, :waited-ms n}, the
  statuses as last seen; the payment's isn't read once the invoice is Received."
  [from to hash]
  (let [start    (System/currentTimeMillis)
        deadline (+ start received-timeout-ms)]
    (loop []
      (let [inv (status to "get_invoice" hash)
            pay (when-not (= "Received" inv) (status from "get_payment" hash))
            now (System/currentTimeMillis)]
        (if (or (#{"Received" "Paid" "Cancelled" "Expired"} inv)
                (client/terminal-statuses pay)
                (< deadline now))
          {:invoice inv, :payment pay, :waited-ms (- now start)}
          (do (Thread/sleep poll-ms)
              (recur)))))))

(defn hold!
  "Waits until the receiver holds the payment of hold invoice `rec`, keeps it
  held for the plan's time, then makes the receiver's call. Returns what
  happened, for the op's :hold: the plan, and
    :received-ms  how long the receiver took to hold the payment;
    :result       :settled or :cancelled, or [:settle e] or [:cancel e] when the
                  call failed (:resolve-holds makes up for it);
    :not-held     in their place when the receiver never held it in time: the
                  invoice's and the payment's status then;
    :early-success when the sender reported Success with no preimage released."
  [test {:keys [id from to hash action hold-ms] :as rec}]
  (let [w    (await-received from to hash)
        base {:action action, :hold-ms hold-ms}]
    (if (not= "Received" (:invoice w))
      (do (when (= "Success" (:payment w))
            (note! test id {:early-success true}))
          (cond-> (assoc base :not-held (select-keys w [:invoice :payment]))
            (= "Success" (:payment w)) (assoc :early-success true)))
      (let [base (assoc base :received-ms (:waited-ms w))]
        (note! test id {:held? true})
        (Thread/sleep hold-ms)
        ; Nobody has the preimage yet, so the sender can't know the payment's end.
        (if (= "Success" (status from "get_payment" hash))
          (do (note! test id {:early-success true})
              (assoc base :early-success true))
          (assoc base :result (act! test rec)))))))

(defmethod client/send-payment! :hold
  [test node to op]
  (let [{:keys [amount id]} (:value op)
        preimage            (invoice/preimage id)
        hash                (payment-hash preimage)
        plan                (plan test id)
        inv                 (invoice/new-invoice! to amount id (:final-tlc-expiry-delta test)
                                                  {:payment_hash hash, :hash_algorithm "sha256"})]
    (if-let [err (:error inv)]
      ; The sender was never asked, so it certainly created no payment.
      {::client/definite [:new-invoice err]}
      (let [rec (record! test (merge {:id id, :from node, :to to, :amount amount
                                      :hash hash, :preimage preimage}
                                     plan))
            res (client/call-send-payment! test node {:invoice (:invoice_address inv)} id)]
        (assoc res ::client/value
               {:hold (if (:payment_hash res)
                        (hold! test rec)
                        ; No payment, or none the client knows of: :resolve-holds
                        ; ends the invoice.
                        (assoc plan :not-held :not-sent))})))))

(def over
  "Results after which a hold invoice needs no more calls."
  #{:settled :cancelled})

(defn resolve-one!
  "Ends hold invoice `rec`, whose call a fault prevented or whose payment came
  late: the plan's call when the receiver holds the payment (Received), a cancel
  when nothing has arrived (Open). Returns what it did: act!'s or cancel!'s
  result, :over when the invoice had ended by itself, or [:unread error]."
  [test {:keys [to hash] :as rec}]
  (let [st (attempt #(:status (rpc/call to "get_invoice" {:payment_hash hash})))]
    (cond (:error st)              [:unread (:error st)]
          (= "Received" (:ok st)) (act! test rec)
          (= "Open" (:ok st))     (cancel! test rec)
          :else                   :over)))

(defn- reachable?
  "Whether `node` answers within reach-timeout-ms."
  [node]
  (try (await-fn #(rpc/call node "node_info")
                 {:log-message (str "Waiting for " node " to end its hold invoices")
                  :timeout     reach-timeout-ms})
       true
       (catch Exception e
         (warn e "Couldn't reach" node "to end its hold invoices")
         false)))

(defn resolve-all!
  "resolve-one! for every hold invoice not known to be over, receiver by
  receiver. Returns {receiver {what-was-done count}}; a receiver that doesn't
  answer is waited for once, not once per invoice."
  [test]
  (let [pending (->> (vals @(:holds test))
                     (remove (comp over :result))
                     (sort-by :id)
                     (group-by :to))]
    (into (sorted-map)
          (for [[node recs] pending]
            [node (if (reachable? node)
                    (frequencies (map #(let [r (resolve-one! test %)]
                                         (if (vector? r) (first r) r))
                                      recs))
                    {:unreachable (count recs)})]))))

(defmethod client/invoke-op :resolve-holds
  [test _node op]
  (assoc op :type :ok, :value (resolve-all! test)))

(defn resolve-generator
  "One :resolve-holds, when the test sends hold-invoice payments."
  [opts]
  (when (enabled? opts)
    (gen/clients (gen/once {:f :resolve-holds}))))

(defmethod client/invoke-op :read-holds
  [test _node op]
  (assoc op :type :ok, :value (mapv #(mpp/read-invoice (dissoc % :preimage))
                                    (sort-by :id (vals @(:holds test))))))

(defn outcome
  "How one hold-invoice payment ended, from its :read-holds row:
    :paid        Success at the sender, Paid at the receiver, after a settle;
    :unpaid      Failed (or never created) at the sender, nothing settled at
                 the receiver;
    :held        the receiver still holds the payment (Received);
    :unfinished  the sender's payment is still Created or Inflight;
    :unread      a status couldn't be read;
  and the violations:
    :success-before-settle     the sender reported Success while the receiver
                               held the payment and no preimage was released;
    :success-without-preimage  Success, and the receiver never got the preimage;
    :paid-without-preimage     the invoice is Paid, and it never got it;
    :paid-but-failed, :success-but-unpaid  the two ends disagree."
  [{:keys [invoice-status payment-status no-payment released? early-success]}]
  (let [success? (= "Success" payment-status)
        paid?    (= "Paid" invoice-status)]
    (cond
      early-success                                :success-before-settle
      (nil? invoice-status)                        :unread
      (and (nil? payment-status) (not no-payment)) :unread
      (and success? (not released?))               :success-without-preimage
      (and paid? (not released?))                  :paid-without-preimage
      (= "Received" invoice-status)                :held
      (#{"Created" "Inflight"} payment-status)     :unfinished
      (and success? paid?)                         :paid
      success?                                     :success-but-unpaid
      paid?                                        :paid-but-failed
      (mpp/unpaid-invoice invoice-status)          :unpaid
      :else                                        :unread)))

(def violations
  #{:success-before-settle :success-without-preimage :paid-without-preimage
    :paid-but-failed :success-but-unpaid})

(defn analyze
  [history]
  (let [attempts (filter #(and (= :pay (:f %))
                               (= :hold (get-in % [:value :kind]))
                               (#{:ok :fail :info} (:type %)))
                         history)
        read     (->> history
                      (filter #(and (= :read-holds (:f %)) (= :ok (:type %))))
                      last
                      :value)
        rows     (map #(assoc % :outcome (outcome %)) read)
        by       (group-by :outcome rows)
        counts   (fn [rs] (into (sorted-map) (frequencies (map :outcome rs))))
        brief    #(select-keys % [:id :from :to :amount :hash :action :hold-ms :held? :released?
                                  :result :outcome :invoice-status :payment-status :payment-error
                                  :invoice-error :invoice-unread :payment-unread])
        bad      (mapcat by (sort violations))
        open     (mapcat by [:held :unfinished :unread])
        held     (filter :held? rows)]
    {:valid?     (cond (seq bad)     false
                       (nil? read)   :unknown
                       (seq open)    :unknown
                       ; Nothing was ever held: the kind wasn't exercised.
                       (empty? held) :unknown
                       :else         true)
     :attempts   (count attempts)
     :invoices   (count rows)
     ; Attempts whose receiver made no invoice: nothing was sent.
     :no-invoice (- (count attempts) (count rows))
     ; Payments a receiver held, and of those the ones whose settle or cancel
     ; had to wait for :resolve-holds or never happened.
     :held       (count held)
     :outcomes   (counts rows)
     :by-action  (into (sorted-map)
                       (update-vals (group-by :action held) counts))
     :violations (mapv brief bad)
     :not-final  (mapv brief open)}))

(defn checker
  "Each hold-invoice payment ends Success with its invoice Paid, after the
  harness released the preimage, or Failed with the invoice unpaid. Invalid
  when the ends disagree or money moved without the preimage; :unknown while a
  payment is unfinished or still held, without the read, or when no receiver
  ever held a payment."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (analyze history))))
