(ns jepsen.fiber.schedule
  "Payments and faults drawn in advance from the test's :seed, so that one seed
  gives a run the same operations.

  Jepsen asks the generator for the next operation whenever an operation
  completes. If that operation isn't due yet, Jepsen drops the answer and asks
  again later, from the generator as it was. A generator that calls a random
  function, as this suite's payments did and jepsen.nemesis.combined's generators
  do, draws new values on every ask, and how often Jepsen asks depends on when operations
  complete. With such generators a seed fixes neither the payments nor the faults:
  under Jepsen's own simulator (jepsen.generator.test/simulate, fixed random
  values), the same payment generator issues different amounts when only the
  latency changes.

  Here every random choice comes from a lazy sequence drawn once, from its own
  SplittableRandom split off one seeded with :seed: payment attempt ids, amounts
  and destinations; fault kinds and targets; and the gaps between operations.
  Asking again returns the same next operation, so the same seed gives each stream
  the same operations in the same order. Timing still decides when each operation
  runs, how the streams interleave, which client sends each payment (the first one
  free), and everything inside Fiber and ckb."
  (:require [jepsen.generator :as gen])
  (:import (java.util SplittableRandom)))

(def max-gap-s
  "The longest gap between two operations of one stream, as in gen/stagger."
  100)

(defn new-seed
  "A seed for a run that wasn't given one with --seed."
  []
  (.nextLong (SplittableRandom.) Long/MAX_VALUE))

(def stream-names
  "The streams of the payments and of the first faults, split off the seeded root
  in this order. The list is closed: a new stream gets its own root from its name
  (see `stream`), so the streams listed here, and the operations a seed gave
  before, stay as they were."
  [:pay :pay-gaps :pay-after-close :pay-after-close-gaps
   :partition :partition-gaps :db :db-gaps :disconnect :disconnect-gaps])

(def ^:private golden-gamma
  "0x9E3779B97F4A7C15 as a signed long, the increment SplittableRandom uses."
  -7046029254386353131)

(defn streams
  "The random streams of one run: stream name to its SplittableRandom for the
  names in stream-names, plus :seed. Take a stream with `stream`."
  [seed]
  (let [root (SplittableRandom. (long seed))]
    (into {:seed (long seed)} (map (fn [k] [k (.split root)])) stream-names)))

(defn stream
  "The SplittableRandom for stream `k` of `streams`. A name in stream-names gets
  the stream split off the seeded root. Any other name gets a stream of its own,
  split off a root seeded with the run's seed and the name, so adding a fault
  changes no other fault's operations. Each stream should feed one sequence:
  the instance is shared, and drawing from it advances it."
  ^SplittableRandom [streams k]
  (or (get streams k)
      (.split (SplittableRandom.
                (unchecked-add (long (:seed streams))
                               (unchecked-multiply golden-gamma
                                                   (long (.hashCode (name k)))))))))

(defn gaps-stream
  "The stream of the gaps between the operations of stream `k`."
  ^SplittableRandom [streams k]
  (stream streams (keyword (str (name k) "-gaps"))))

(defn pick
  "A uniformly chosen element of `coll`, drawn from `r`."
  [^SplittableRandom r coll]
  (nth coll (.nextInt r (count coll))))

(defn gaps
  "Endless gaps between operations in nanoseconds: exponentially distributed with
  a mean of `mean-s` seconds and capped at max-gap-s, like gen/stagger's."
  [^SplittableRandom r mean-s]
  (repeatedly
    #(long (* 1e9 (min max-gap-s
                       (* (- (double mean-s)) (Math/log (- 1.0 (.nextDouble r)))))))))

(defrecord Paced [gaps next-time gen]
  gen/Generator
  (op [_ test ctx]
    (when-let [[op gen'] (gen/op gen test ctx)]
      (let [next-time (or next-time (:time ctx))]
        (cond (identical? :pending op)
              [op (Paced. gaps next-time gen')]

              ; Due now: issue it, and schedule the next one a gap after it.
              (<= next-time (:time op))
              [op (Paced. (rest gaps) (+ (:time op) (first gaps)) gen')]

              ; Not due yet. Jepsen will ask again from this Paced, which still
              ; holds the same gaps.
              :else
              [(assoc op :time next-time)
               (Paced. (rest gaps) (+ next-time (first gaps)) gen')]))))

  (update [_ test ctx event]
    (Paced. gaps next-time (gen/update gen test ctx event))))

(defn paced
  "Like gen/stagger, but takes the gaps from the sequence `gaps` instead of
  drawing one each time it is asked."
  [gaps gen]
  (when gen
    (Paced. gaps nil gen)))

(defn payments
  "Endless payment ops for a test with `node-count` nodes. :to-index picks the
  destination among the nodes other than the sender, in the order of :nodes."
  [^SplittableRandom r node-count]
  (repeatedly
    (fn []
      (let [id     (.nextLong r Long/MAX_VALUE)
            amount (* 1000000 (inc (.nextLong r 20)))
            to     (.nextLong r (max 1 (dec node-count)))]
        {:f :pay, :value {:id id, :amount amount, :to-index to}}))))

(defn with-kinds
  "Payment ops `ps`, each with a :kind drawn uniformly from `kinds` with `r` (a
  kind listed twice is drawn twice as often). Nothing is drawn for a single kind,
  and keysend alone, the default, adds no :kind, so keysend runs keep their ops."
  [^SplittableRandom r kinds ps]
  (let [kinds (vec kinds)]
    (cond (every? #{:keysend} kinds) ps
          (apply = kinds)            (map #(assoc-in % [:value :kind] (first kinds)) ps)
          :else                      (map #(assoc-in % [:value :kind] (pick r kinds)) ps))))

(defn kinds-stream
  "The stream of the payment kinds of payment stream `k`."
  ^SplittableRandom [streams k]
  (stream streams (keyword (str (name k) "-kind"))))

(defn paced-payments
  "Payments from stream `k`, with gaps averaging 1/rate seconds, each of a kind
  drawn from `kinds` (default keysend; see with-kinds)."
  ([streams k rate node-count]
   (paced-payments streams k rate node-count [:keysend]))
  ([streams k rate node-count kinds]
   (paced (gaps (gaps-stream streams k) (/ 1.0 rate))
          (with-kinds (kinds-stream streams k) kinds
                      (payments (stream streams k) node-count)))))
