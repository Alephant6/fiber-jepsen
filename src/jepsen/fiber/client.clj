(ns jepsen.fiber.client
  "Sends keysend payments from the client's node to another node, and performs
  the final read of every node's channels and every payment's status."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [warn]]
            [jepsen [client :as client]
                    [random :as rand]
                    [util :refer [await-fn]]]
            [jepsen.fiber [rpc :as rpc]
                          [topology :as topology]])
  (:import (java.io IOException)
           (java.net ConnectException)
           (org.apache.http.conn ConnectTimeoutException)))

(def terminal-statuses #{"Success" "Failed"})

(def payment-timeout-ms
  "How long a client waits for a payment to finish before recording it as :info."
  15000)

(def attempt-record-key
  "With --ledger, every payment carries its attempt id in this custom record (keys up
  to 65535 are left to applications), so the final read can match each payment a
  node holds to the operation that asked for it, even when the client never saw
  the send_payment reply."
  0xf1be)

(defn attempt-id
  "A random, non-negative id for one payment attempt. Payments get theirs from
  jepsen.fiber.schedule; this one is for the post-heal probes."
  []
  (rand/long Long/MAX_VALUE))

(defn attempt-records
  "send_payment's custom_records, tagging the payment with attempt `id`."
  [id]
  {(str "0x" (Long/toHexString attempt-record-key)) (format "0x%016x" id)})

(defn records->attempt-id
  "The attempt id in a payment's custom_records as rpc/call returns them (keys
  keywordized, e.g. :0xf1be), or nil."
  [records]
  (some-> (get records (keyword (str "0x" (Long/toHexString attempt-record-key))))
          rpc/hex->int
          long))

(defn await-payment
  "Polls get_payment until the payment is Success or Failed, or the timeout
  passes. Returns the last result seen, which may be nil."
  [node hash]
  (let [deadline (+ (System/currentTimeMillis) payment-timeout-ms)]
    (loop [last-seen nil]
      (let [res (try (rpc/call node "get_payment" {:payment_hash hash})
                     (catch Exception _ last-seen))]
        (if (or (terminal-statuses (:status res))
                (< deadline (System/currentTimeMillis)))
          res
          (do (Thread/sleep 200)
              (recur res)))))))

(defn call-send-payment!
  "Calls send_payment on `node` with `params`; with --ledger the payment carries
  attempt `id`. Returns send_payment's result, or
    {::definite error}    the node certainly created no payment;
    {::indefinite error}  it may have: the call timed out or the connection dropped."
  [test node params id]
  (try
    (rpc/call node "send_payment"
              (cond-> params
                (and (:ledger test) id) (assoc :custom_records (attempt-records id))))
    ; Nothing reached the node, so the payment definitely wasn't created.
    (catch ConnectException _ {::definite :node-down})
    (catch ConnectTimeoutException _ {::definite :node-unreachable})
    (catch clojure.lang.ExceptionInfo e
      (if (= :rpc-error (:type (ex-data e)))
        {::definite (ex-message e)}
        (throw e)))
    (catch IOException e
      {::indefinite [:no-reply (.getSimpleName (class e))]})))

(defn send-keysend!
  "Asks `node` to pay `amount` shannons to `to` by keysend, and returns as
  call-send-payment! does."
  [test node to amount id]
  (call-send-payment! test node
                      {:target_pubkey (get @(:pubkeys test) to)
                       :amount        (rpc/int->hex amount)
                       :keysend       true}
                      id))

(defn destination
  "Where `node` sends the payment in `op`: its :to if that is another node, else
  the other node at :to-index (drawn by jepsen.fiber.schedule), else a random one."
  [test node op]
  (let [{:keys [to to-index]} (:value op)
        others                (vec (remove #{node} (:nodes test)))]
    (cond (and to (not= to node)) to
          to-index                (nth others (mod to-index (count others)))
          :else                   (rand/nth others))))

(defmulti send-payment!
  "Sends the payment in `op` from `node` to `to`, and returns as send-keysend!
  does. Dispatches on the payment's :kind, keysend when it has none, so that a
  workload can add a kind (invoice, MPP) with a method in its own namespace."
  (fn [_test _node _to op] (get-in op [:value :kind] :keysend)))

(defmethod send-payment! :keysend
  [test node to op]
  (send-keysend! test node to (get-in op [:value :amount]) (get-in op [:value :id])))

(defn pay!
  [test node op]
  (let [to     (destination test node op)
        value  (-> (:value op) (dissoc :to-index) (assoc :from node :to to))
        res    (send-payment! test node to op)]
    (cond
      (::definite res)
      (assoc op :type :fail, :value value, :error (::definite res))

      (::indefinite res)
      (assoc op :type :info, :value value, :error (::indefinite res))

      :else
      (let [hash  (:payment_hash res)
            value (assoc value :hash hash)]
        (swap! (:payments test) conj {:node node, :hash hash})
        (let [final (await-payment node hash)]
          (case (:status final)
            "Success" (assoc op :type :ok, :value (assoc value :fee (rpc/hex->int (:fee final))))
            "Failed"  (assoc op :type :fail, :value value, :error (:failed_error final))
            (assoc op :type :info, :value value, :error [:not-finished (:status final)])))))))

(defn probe!
  "After the network has healed, sends one small keysend each way over every
  channel, and with --probe-routes one between every two nodes without a channel
  (marked :route? true; see topology/probe-pairs). A healthy network completes
  them; a wedged channel leaves them stuck. Status is Rejected when the node
  certainly created no payment, and Unknown when the call got no reply."
  [test]
  (vec (for [[from to route?] (topology/probe-pairs test)
             :let             [amount 1000000
                               id     (attempt-id)]]
         (let [res   (try (send-keysend! test from to amount id)
                          (catch Exception e {::indefinite (ex-message e)}))
               entry (cond-> {:from from, :to to, :amount amount, :id id}
                       route? (assoc :route? true))
               ; The checker matches errors as strings.
               err   #(if (string? %) % (pr-str %))]
           (cond
             (::definite res)
             (assoc entry :status "Rejected", :error (err (::definite res)))

             (::indefinite res)
             (assoc entry :status "Unknown", :error (err (::indefinite res)))

             :else
             (let [final (await-payment from (:payment_hash res))]
               (assoc entry
                      :hash   (:payment_hash res)
                      :status (:status final)
                      :error  (:failed_error final))))))))

(defn channel-between
  "The id of the channel opened between nodes a and b during setup."
  [test a b]
  (let [ids (fn [n] (set (map :channel_id (get @(:initial-channels test) n))))]
    (first (set/intersection (ids a) (ids b)))))

(defn close!
  "Asks every channel to close cooperatively (--close). Faults keep running, so it
  retries for about 20 s, alternating between the two ends while one is down."
  [test]
  (vec (for [[a b] (topology/edges test)]
         (let [ch      (channel-between test a b)
               attempt (fn [node]
                         (try (rpc/call node "shutdown_channel"
                                        {:channel_id ch, :fee_rate (rpc/int->hex 1000)}
                                        {:timeout 10000})
                              :ok
                              (catch Exception e (or (ex-message e) (str (class e))))))]
           (loop [tries 1]
             (let [node   (if (odd? tries) a b)
                   result (attempt node)]
               (if (or (= :ok result) (<= 20 tries))
                 {:channel ch, :by node, :result result, :tries tries}
                 (do (Thread/sleep 1000)
                     (recur (inc tries))))))))))

(defn read-node
  "Every channel of one node. Retries while the node recovers from faults."
  [node]
  (await-fn #(:channels (rpc/call node "list_channels" {:include_closed true}))
            {:log-message (str "Waiting to read channels from " node)
             :timeout     120000}))

(def list-page-size
  "list_payments returns at most 500 payments per call."
  500)

(defn listed-payment
  "The parts of a list_payments entry the ledger needs. The status goes under
  :payment-status so that scripts/repeat.sh, which counts `:status \"Inflight\"`
  in the log, still counts only what it did before --ledger."
  [p]
  {:hash           (:payment_hash p)
   :id             (records->attempt-id (:custom_records p))
   :payment-status (:status p)
   :fee            (or (rpc/hex->int (:fee p)) 0)
   :error          (:failed_error p)})

(defn list-payments
  "Every payment session on `node` (the payments it sent), following list_payments'
  cursor."
  [node]
  (loop [after nil, acc []]
    (let [res  (rpc/call node "list_payments"
                         (cond-> {:limit (rpc/int->hex list-page-size)}
                           after (assoc :after after))
                         {:timeout 30000})
          page (:payments res)
          acc  (into acc (map listed-payment) page)]
      (if (and (= list-page-size (count page)) (:last_cursor res))
        (recur (:last_cursor res) acc)
        acc))))

(defn read-payments
  "list-payments, retried while the node recovers from faults; {:error message} if
  the node rejects the call (an fnn without list_payments, for example)."
  [node]
  (await-fn #(try (list-payments node)
                  (catch clojure.lang.ExceptionInfo e
                    (if (= :rpc-error (:type (ex-data e)))
                      {:error (ex-message e)}
                      (throw e))))
            {:log-message (str "Waiting to list payments on " node)
             :timeout     120000}))

(defn read-all
  [test]
  (cond-> {:channels (into {} (for [n (:nodes test)] [n (read-node n)]))
           :payments (vec (for [{:keys [node hash]} @(:payments test)]
                            (let [res (try (rpc/call node "get_payment" {:payment_hash hash})
                                           (catch Exception e
                                             (warn e "Couldn't read payment" hash "from" node)
                                             nil))]
                              {:node   node
                               :hash   hash
                               :status (:status res)
                               :error  (:failed_error res)})))}
    ; With --ledger: every payment each node holds, including ones the client never
    ; got a hash for.
    (:ledger test) (assoc :listed (into {} (for [n (:nodes test)] [n (read-payments n)])))))

(defmulti invoke-op
  "Performs a client operation this namespace doesn't know, by its :f. A workload
  with operations of its own (opening channels, swaps) adds methods in its own
  namespace, which the test must load."
  (fn [_test _node op] (:f op)))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ _test])

  (invoke! [_ test op]
    (case (:f op)
      :pay   (pay! test node op)
      :close (assoc op :type :info, :value (close! test))
      :probe (assoc op :type :ok, :value (probe! test))
      :read  (assoc op :type :ok, :value (read-all test))
      (invoke-op test node op)))

  (teardown! [_ _test])

  (close! [_ _test]))

(defn client
  []
  (Client. nil))
