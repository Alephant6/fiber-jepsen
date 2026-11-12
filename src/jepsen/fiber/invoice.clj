(ns jepsen.fiber.invoice
  "Invoice payments (--pay-kinds invoice): the receiver creates an invoice with
  new_invoice, and the sender pays it with send_payment. Unlike keysend, the
  receiver must be up to be paid, the invoice fixes the amount and the payment
  hash, and the receiver settles with a preimage it stored in advance.

  With --ledger the payment carries its attempt id in a custom record, as keysend
  payments do: send_payment passes custom records through for invoices too."
  (:require [jepsen.fiber [client :as client]
                          [rpc :as rpc]])
  (:import (java.io IOException)
           (java.net ConnectException)
           (java.nio.charset StandardCharsets)
           (java.security MessageDigest SecureRandom)
           (org.apache.http.conn ConnectTimeoutException)))

(def currency
  "The devnet's invoice currency (Fiber's tests/bruno uses it)."
  "Fibd")

(def expiry-s
  "Invoices outlive the longest run."
  3600)

(defn preimage
  "The invoice's preimage, 32 bytes as 0x-prefixed hex: derived from attempt `id`,
  so a seed gives the same payment hashes again, or random without one."
  [id]
  (let [bytes (if id
                (.digest (MessageDigest/getInstance "SHA-256")
                         (.getBytes (str "fiber-jepsen invoice " id) StandardCharsets/UTF_8))
                (let [b (byte-array 32)] (.nextBytes (SecureRandom.) b) b))]
    (str "0x" (apply str (map #(format "%02x" %) bytes)))))

(defn new-invoice!
  "Asks `node` for an invoice of `amount` shannons. Returns new_invoice's result,
  or {:error e} when it made none (the receiver is down or refused). With
  `final-delta` (ms, --final-tlc-expiry-delta) the invoice asks for that final
  TLC expiry delta; without it Fiber's default applies. `extra` adds parameters
  (the MPP kind's allow_mpp). With a payment_hash in `extra` the node isn't given
  the preimage: that makes a hold invoice (jepsen.fiber.hold)."
  [node amount id final-delta & [extra]]
  (try
    (rpc/call node "new_invoice"
              (cond-> (merge {:amount      (rpc/int->hex amount)
                              :currency    currency
                              :description (str "fiber-jepsen " id)
                              :expiry      (rpc/int->hex expiry-s)}
                             extra)
                (not (:payment_hash extra)) (assoc :payment_preimage (preimage id))
                final-delta                 (assoc :final_expiry_delta (rpc/int->hex final-delta))))
    (catch ConnectException _ {:error :receiver-down})
    (catch ConnectTimeoutException _ {:error :receiver-unreachable})
    (catch clojure.lang.ExceptionInfo e
      (if (= :rpc-error (:type (ex-data e)))
        {:error (ex-message e)}
        (throw e)))
    (catch IOException e
      {:error [:no-reply (.getSimpleName (class e))]})))

(defmethod client/send-payment! :invoice
  [test node to op]
  (let [{:keys [amount id]} (:value op)
        inv                 (new-invoice! to amount id (:final-tlc-expiry-delta test))]
    (if-let [err (:error inv)]
      ; The sender was never asked, so it certainly created no payment.
      {::client/definite [:new-invoice err]}
      (client/call-send-payment! test node {:invoice (:invoice_address inv)} id))))
