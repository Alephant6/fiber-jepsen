(ns jepsen.fiber.client
  "Sends keysend payments from the client's node to another node, and performs
  the final read of every node's channels and every payment's status."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [client :as client]
                    [util :refer [await-fn]]]
            [jepsen.fiber.rpc :as rpc])
  (:import (java.net ConnectException)))

(def terminal-statuses #{"Success" "Failed"})

(def payment-timeout-ms
  "How long a client waits for a payment to finish before recording it as :info."
  15000)

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

(defn pay!
  [test node op]
  (let [to     (let [t (get-in op [:value :to])]
                 (if (or (nil? t) (= t node))
                   (rand-nth (remove #{node} (:nodes test)))
                   t))
        amount (get-in op [:value :amount])
        value  (assoc (:value op) :from node :to to)
        res    (try
                 (rpc/call node "send_payment"
                           {:target_pubkey (get @(:pubkeys test) to)
                            :amount        (rpc/int->hex amount)
                            :keysend       true})
                 ; Nothing reached the node, so the payment definitely wasn't created.
                 (catch ConnectException _ ::node-down)
                 (catch clojure.lang.ExceptionInfo e
                   (if (= :rpc-error (:type (ex-data e)))
                     {::rejected (ex-message e)}
                     (throw e))))]
    (cond
      (= ::node-down res)
      (assoc op :type :fail, :value value, :error :node-down)

      (::rejected res)
      (assoc op :type :fail, :value value, :error (::rejected res))

      :else
      (let [hash  (:payment_hash res)
            value (assoc value :hash hash)]
        (swap! (:payments test) conj {:node node, :hash hash})
        (let [final (await-payment node hash)]
          (case (:status final)
            "Success" (assoc op :type :ok, :value (assoc value :fee (rpc/hex->int (:fee final))))
            "Failed"  (assoc op :type :fail, :value value, :error (:failed_error final))
            (assoc op :type :info, :value value, :error [:not-finished (:status final)])))))))

(defn read-node
  "Every channel of one node. Retries while the node recovers from faults."
  [node]
  (await-fn #(:channels (rpc/call node "list_channels" {:include_closed true}))
            {:log-message (str "Waiting to read channels from " node)
             :timeout     120000}))

(defn read-all
  [test]
  {:channels (into {} (for [n (:nodes test)] [n (read-node n)]))
   :payments (vec (for [{:keys [node hash]} @(:payments test)]
                    (let [res (try (rpc/call node "get_payment" {:payment_hash hash})
                                   (catch Exception e
                                     (warn e "Couldn't read payment" hash "from" node)
                                     nil))]
                      {:node   node
                       :hash   hash
                       :status (:status res)
                       :error  (:failed_error res)})))})

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_ _test])

  (invoke! [_ test op]
    (case (:f op)
      :pay  (pay! test node op)
      :read (assoc op :type :ok, :value (read-all test))))

  (teardown! [_ _test])

  (close! [_ _test]))

(defn client
  []
  (Client. nil))
