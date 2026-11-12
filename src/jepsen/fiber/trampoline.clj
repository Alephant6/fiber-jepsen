(ns jepsen.fiber.trampoline
  "Trampoline payments (--pay-kinds trampoline). The sender doesn't find the
  whole route. It names trampoline nodes (send_payment's trampoline_hops),
  finds a route to the first of them only, and hands it the rest in an inner
  onion: each trampoline node finds its own route to the next one, and the
  last to the receiver. So a payment's forwarding is no longer one onion that
  every hop just peels: a trampoline node starts a payment of its own for the
  next leg and may retry it, while it holds the TLC that pays it for the first.

  The receiver creates an invoice that allows trampoline routing (new_invoice
  with allow_trampoline_routing). The trampoline nodes of a payment are
  --trampoline-hops of the test's other nodes (default 1; with a range, each
  payment's count is drawn from it), drawn from the attempt id alone (`hops`),
  so the kind draws nothing from the seed's streams.
  Fiber wants max_fee_amount with trampoline_hops and spends as much of it as
  it can, so the fee is a budget rather than what the route costs: fee-percent
  of the amount per trampoline node.

  The usual checks apply: every payment ends Success or Failed, no TLC is left,
  and with --ledger each node's balance change matches the payments (a node can
  now forward a payment it sent or received, which jepsen.fiber.ledger allows
  for this kind). The checker here only counts, and is :unknown when no
  trampoline payment succeeded: then the kind wasn't exercised."
  (:require [jepsen.checker :as checker]
            [jepsen.fiber [client :as client]
                          [invoice :as invoice]
                          [rpc :as rpc]])
  (:import (java.util SplittableRandom)))

(def fee-percent
  "The fee budget of a payment, percent of its amount per trampoline node.
  Fiber estimates a trampoline node's fee at 0.1% (its default forwarding fee)
  to 1% of the amount, and the hops on the way to the first trampoline node
  take theirs from the same budget."
  1)

(defn enabled?
  "Whether the test sends trampoline payments."
  [opts]
  (boolean (some #{:trampoline} (:pay-kinds opts))))

(def default-hops
  "--trampoline-hops' default: one trampoline node."
  [1 1])

(defn hops
  "The trampoline nodes of attempt `id` from `from` to `to`, in order:
  --trampoline-hops of the other nodes (a count drawn from the range), or all of
  them if they are fewer, drawn from the id alone. Empty when the test has no
  third node."
  [test from to id]
  (let [others  (vec (remove #{from to} (:nodes test)))
        r       (SplittableRandom. (long (or id 0)))
        [lo hi] (:trampoline-hops test default-hops)
        n       (min (+ lo (.nextInt r (inc (- hi lo)))) (count others))]
    (loop [pool others, out []]
      (if (= n (count out))
        out
        (let [i (.nextInt r (count pool))]
          (recur (into (subvec pool 0 i) (subvec pool (inc i)))
                 (conj out (nth pool i))))))))

(defn max-fee
  "The fee budget in shannons for `amount` over `hop-count` trampoline nodes."
  [amount hop-count]
  (max 1 (quot (* amount fee-percent hop-count) 100)))

(defmethod client/send-payment! :trampoline
  [test node to op]
  (let [{:keys [amount id]} (:value op)
        via                 (hops test node to id)]
    (if (empty? via)
      ; Nothing was asked of any node.
      {::client/definite :no-trampoline-node}
      (let [inv (invoice/new-invoice! to amount id (:final-tlc-expiry-delta test)
                                      {:allow_trampoline_routing true})]
        (if-let [err (:error inv)]
          ; The sender was never asked, so it certainly created no payment.
          {::client/definite [:new-invoice err]}
          (assoc (client/call-send-payment! test node
                                            {:invoice         (:invoice_address inv)
                                             :trampoline_hops (mapv #(get @(:pubkeys test) %) via)
                                             :max_fee_amount  (rpc/int->hex (max-fee amount (count via)))}
                                            id)
                 ::client/value {:via via}))))))

(defn analyze
  [history]
  (let [done   (filter #(and (= :pay (:f %))
                             (= :trampoline (get-in % [:value :kind]))
                             (#{:ok :fail :info} (:type %)))
                       history)
        ok     (filter #(= :ok (:type %)) done)
        errors (frequencies (for [op done :when (= :fail (:type op))]
                              (let [e (:error op)] (if (string? e) e (pr-str e)))))
        fee-of (fn [op] (let [{:keys [amount fee]} (:value op)]
                          (when (and amount fee (pos? amount)) (/ (* 100.0 fee) amount))))
        fees   (keep fee-of ok)]
    (cond-> {:valid?   (if (seq ok) true :unknown)
             :attempts (count done)
             :outcomes (into (sorted-map) (frequencies (map :type done)))
             ; How many trampoline nodes the successful payments went through.
             :hops     (into (sorted-map) (frequencies (map #(count (get-in % [:value :via])) ok)))
             ; The commonest reasons a payment failed, most frequent first.
             :errors   (vec (take 5 (sort-by (comp - val) errors)))}
      (seq fees) (assoc :fee-percent {:min (apply min fees), :max (apply max fees)}))))

(defn checker
  "Counts the trampoline payments: outcomes, the trampoline nodes of the
  successful ones, the fees they paid as a percentage of the amount, and why
  the failed ones failed. :unknown when none succeeded."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (analyze history))))
