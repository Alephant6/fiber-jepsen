(ns jepsen.fiber.mpp
  "Multi-part payments (--pay-kinds mpp, C-07). The receiver creates an invoice
  that allows MPP (new_invoice with allow_mpp), and the sender pays it with
  send_payment, which may then split the amount into parts over several routes
  (up to max_parts, --mpp-max-parts; Fiber's default is 12). Fiber splits only
  when no single route can carry the amount, so MPP payments draw their amounts
  from a range of their own (--mpp-amount), which should lie above any one
  channel's balance and below what a node holds over all its channels.

  The receiver holds the parts until their sum reaches the invoice's amount and
  then settles all of them with the preimage; if the set isn't complete in time
  (DEFAULT_HOLD_TLC_TIMEOUT, 120 s in a release build) it fails all of them. So a
  payment should end either Success at the sender with the invoice Paid at the
  receiver, or Failed with the invoice unpaid, never in between.

  Fiber reports a payment's parts only in debug builds (get_payment's routers),
  so while a payment is in flight the client polls the sender's channels for
  TLCs with its payment hash, and records what it saw under :parts: the most
  TLCs pending at once (the parts in flight together), the distinct TLCs (parts
  and retries) and the distinct channels they left on. A set held at the receiver
  stays pending on the sender's channels until it settles, so a split payment
  is usually seen with two or more at once; a part that settles between two polls
  is missed, so the counts are lower bounds. A TLC already removed
  (RemoveAckConfirmed), which Fiber lists until the channel's next update, is
  not counted as in flight.

  After the final read, :read-invoices reads every MPP invoice's status on its
  receiver and the payment's on its sender, and the checker here wants the two
  to agree. Whether the receiver got the full amount, not a part of it, is what
  --ledger checks: a node's balance change must match the payments it sent and
  received."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.checker :as checker]
            [jepsen.util :refer [await-fn]]
            [jepsen.fiber [client :as client]
                          [invoice :as invoice]
                          [rpc :as rpc]]))

(def default-amount-ckb
  "--mpp-amount's default, CKB: above a setup channel's balance on either side
  (about 900 CKB of its 1000 CKB, less the reserve), so that a payment must
  split, and below a node's balance over three channels."
  [1000 1600])

(def watch-timeout-ms
  "How long the client watches an MPP payment's parts before handing it to the
  usual wait (client/await-payment, 15 s more). Every hop of every part takes a
  few commitment round trips, and with 0.5-3 s of delay per packet a part takes
  tens of seconds."
  45000)

(def poll-ms 250)

(defn enabled?
  "Whether the test sends MPP payments."
  [opts]
  (boolean (some #{:mpp} (:pay-kinds opts))))

(defn amount-ranges
  "{kind [lo hi]} in shannons, for schedule/paced-payments: the MPP range, when
  the test sends MPP payments."
  [opts]
  (when (enabled? opts)
    (let [[lo hi] (:mpp-amount opts default-amount-ckb)]
      {:mpp [(* lo 100000000) (* hi 100000000)]})))

(defn removed?
  "Whether a pending_tlcs entry is a TLC this end has already removed (status
  RemoveAckConfirmed, inbound or outbound). Fiber lists such a TLC until the
  channel's next commitment_signed, so a part that failed or settled can still
  be listed next to the retry that replaced it."
  [tlc]
  (let [st (:status tlc)]
    (= "RemoveAckConfirmed" (if (map? st) (first (vals st)) st))))

(defn tlcs
  "The pending TLCs for payment `hash` on `channels` (list_channels), as
  {:channel :id :amount :removed?}."
  [channels hash]
  (for [ch    channels
        t     (:pending_tlcs ch)
        :when (= hash (:payment_hash t))]
    {:channel  (:channel_id ch)
     :id       (rpc/hex->int (:id t))
     :amount   (rpc/hex->int (:amount t))
     :removed? (removed? t)}))

(defn add-sample
  "The parts seen so far, `parts`, with one poll's TLCs `ts` added. A removed TLC
  counts as a part seen, but not as one in flight."
  [parts ts]
  (let [seen (into (:seen parts #{}) (map (juxt :channel :id)) ts)
        live (remove :removed? ts)]
    {:seen        seen
     :max-at-once (max (:max-at-once parts 0) (count live))
     :max-amount  (max (:max-amount parts 0) (reduce + 0 (map :amount live)))}))

(defn summary
  "What :parts records: the most TLCs pending at once and their largest summed
  amount, the distinct TLCs and the distinct channels."
  [parts]
  {:max-at-once (:max-at-once parts 0)
   :max-amount  (:max-amount parts 0)
   :tlcs        (count (:seen parts))
   :channels    (count (set (map first (:seen parts))))})

(defn watch-parts!
  "Polls `node` (the sender) until payment `hash` is Success or Failed or
  watch-timeout-ms passes, sampling its channels' pending TLCs for the hash each
  time. Returns the parts' summary."
  [node hash]
  (let [deadline (+ (System/currentTimeMillis) watch-timeout-ms)]
    (loop [parts {}]
      (let [ts     (try (tlcs (:channels (rpc/call node "list_channels" {})) hash)
                        (catch Exception _ nil))
            parts  (add-sample parts ts)
            status (try (:status (rpc/call node "get_payment" {:payment_hash hash}))
                        (catch Exception _ nil))]
        (if (or (client/terminal-statuses status)
                (< deadline (System/currentTimeMillis)))
          (summary parts)
          (do (Thread/sleep poll-ms)
              (recur parts)))))))

(defn record-invoice!
  "Remembers an MPP invoice for :read-invoices."
  [test invoice]
  (when-let [invoices (:invoices test)]
    (swap! invoices conj invoice)))

(defmethod client/send-payment! :mpp
  [test node to op]
  (let [{:keys [amount id]} (:value op)
        inv                 (invoice/new-invoice! to amount id (:final-tlc-expiry-delta test)
                                                  {:allow_mpp true})]
    (if-let [err (:error inv)]
      ; The sender was never asked, so it certainly created no payment.
      {::client/definite [:new-invoice err]}
      (let [hash (get-in inv [:invoice :data :payment_hash])
            _    (record-invoice! test {:id id, :from node, :to to, :amount amount, :hash hash})
            res  (client/call-send-payment! test node
                                            (cond-> {:invoice (:invoice_address inv)}
                                              (:mpp-max-parts test)
                                              (assoc :max_parts (rpc/int->hex (:mpp-max-parts test))))
                                            id)]
        (assoc res ::client/value
               (cond-> {:invoice-hash hash}
                 (:payment_hash res) (assoc :parts (watch-parts! node (:payment_hash res)))))))))

(defn- read-status
  "f's result, retried while the node is unreachable. {:error message} when the
  node rejects the call (get_payment for a payment it never created, say), and
  {:unread message} when it can't be reached."
  [node what f]
  (try
    (await-fn #(try (f)
                    (catch clojure.lang.ExceptionInfo e
                      (if (= :rpc-error (:type (ex-data e)))
                        {:error (ex-message e)}
                        (throw e))))
              {:log-message (str "Waiting to read " what " from " node)
               :timeout     60000})
    (catch Exception e
      (warn e "Couldn't read" what "from" node)
      {:unread (or (ex-message e) (str (class e)))})))

(defn read-invoice
  "One MPP invoice's status on its receiver, and its payment's on its sender."
  [{:keys [from to hash] :as inv}]
  (let [i (read-status to "invoice" #(rpc/call to "get_invoice" {:payment_hash hash}))
        p (read-status from "payment" #(rpc/call from "get_payment" {:payment_hash hash}))]
    (cond-> (assoc inv
                   :invoice-status (:status i)
                   :payment-status (:status p)
                   :fee            (some-> (:fee p) rpc/hex->int long))
      (:error i)         (assoc :invoice-error (:error i))
      (:unread i)        (assoc :invoice-unread (:unread i))
      (:failed_error p)  (assoc :payment-error (:failed_error p))
      ; get_payment refused: the sender holds no such payment.
      (:error p)         (assoc :payment-error (:error p), :no-payment true)
      (:unread p)        (assoc :payment-unread (:unread p)))))

(defmethod client/invoke-op :read-invoices
  [test _node op]
  (assoc op :type :ok, :value (mapv read-invoice @(:invoices test))))

(def unpaid-invoice
  "Invoice statuses that mean the receiver settled nothing."
  #{"Open" "Cancelled" "Expired"})

(defn outcome
  "How one MPP payment ended, from its invoice read:
    :paid        Success at the sender, Paid at the receiver;
    :unpaid      Failed (or never created) at the sender, nothing settled at
                 the receiver;
    :unfinished  the sender's payment is still Created or Inflight;
    :held        the receiver holds a complete set (Received) it hasn't settled;
    :unread      a status couldn't be read;
    :paid-but-failed, :success-but-unpaid  the two ends disagree."
  [{:keys [invoice-status payment-status no-payment]}]
  (let [success? (= "Success" payment-status)]
    (cond
      (nil? invoice-status)                        :unread
      (and (nil? payment-status) (not no-payment)) :unread
      (#{"Created" "Inflight"} payment-status)     :unfinished
      (= "Received" invoice-status)                :held
      (and success? (= "Paid" invoice-status))     :paid
      success?                                     :success-but-unpaid
      (= "Paid" invoice-status)                    :paid-but-failed
      (unpaid-invoice invoice-status)              :unpaid
      :else                                        :unread)))

(def disagreements #{:paid-but-failed :success-but-unpaid})

(defn part-kind
  "What a client saw of a payment's `parts`: :split (two or more TLCs pending at
  once, or TLCs on two or more channels), :one-part, :not-seen (it settled or
  failed between polls) or :not-watched (no payment hash came back)."
  [parts]
  (cond (nil? parts)                                              :not-watched
        (or (<= 2 (:max-at-once parts)) (<= 2 (:channels parts))) :split
        (pos? (:tlcs parts))                                      :one-part
        :else                                                     :not-seen))

(defn split
  "{outcome {part-kind count}}: what the clients saw of the parts of the payments
  that ended `outcomes` ({attempt-id outcome}). A Failed payment seen :split had
  parts in flight before it failed, the case all or nothing is about."
  [history outcomes]
  (let [seen (for [op    history
                   :when (and (= :pay (:f op))
                              (= :mpp (get-in op [:value :kind]))
                              (#{:ok :fail :info} (:type op)))
                   :let  [o (get outcomes (get-in op [:value :id]))]
                   :when (#{:paid :unpaid} o)]
               [o (part-kind (get-in op [:value :parts]))])]
    (into (sorted-map)
          (update-vals (group-by first seen) #(frequencies (map second %))))))

(defn analyze
  [history]
  (let [attempts (filter #(and (= :pay (:f %))
                               (= :mpp (get-in % [:value :kind]))
                               (#{:ok :fail :info} (:type %)))
                         history)
        read     (->> history
                      (filter #(and (= :read-invoices (:f %)) (= :ok (:type %))))
                      last
                      :value)
        rows     (map #(assoc % :outcome (outcome %)) read)
        by       (group-by :outcome rows)
        brief    #(select-keys % [:id :from :to :amount :hash :invoice-status :payment-status
                                  :payment-error :invoice-error :invoice-unread :payment-unread])
        bad      (mapcat by disagreements)
        open     (mapcat by [:unfinished :held :unread])]
    {:valid?      (cond (seq bad)   false
                        (nil? read) :unknown
                        (seq open)  :unknown
                        (empty? rows) :unknown
                        :else       true)
     :attempts    (count attempts)
     :invoices    (count rows)
     ; Attempts whose receiver made no invoice: nothing was sent.
     :no-invoice  (- (count attempts) (count rows))
     :outcomes    (into (sorted-map) (update-vals by count))
     :split       (split history (into {} (map (juxt :id :outcome)) rows))
     :disagree    (mapv brief bad)
     :not-final   (mapv brief open)}))

(defn checker
  "Each MPP payment ends all or nothing: Success at the sender with its invoice
  Paid at the receiver, or Failed with the invoice unpaid. Invalid when the two
  ends disagree; :unknown while a payment is unfinished or a receiver holds an
  unsettled set, or without the invoice read."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (analyze history))))
