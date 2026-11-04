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
  (:require [jepsen.generator :as gen]
            [jepsen.nemesis :as n]
            [jepsen.fiber.topology :as topology])
  (:import (java.util SplittableRandom)))

(def max-gap-s
  "The longest gap between two operations of one stream, as in gen/stagger."
  100)

(defn new-seed
  "A seed for a run that wasn't given one with --seed."
  []
  (.nextLong (SplittableRandom.) Long/MAX_VALUE))

(def stream-names
  "One random stream per sequence. Each is split off the seeded root in this
  order, so adding a stream at the end keeps the others unchanged."
  [:pay :pay-gaps :pay-after-close :pay-after-close-gaps
   :partition :partition-gaps :db :db-gaps :disconnect :disconnect-gaps])

(defn streams
  "A map of stream name to its SplittableRandom, for one run's `seed`."
  [seed]
  (let [root (SplittableRandom. (long seed))]
    (into {} (map (fn [k] [k (.split root)])) stream-names)))

(defn- pick
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

(defn paced-payments
  "Payments from stream `k`, with gaps averaging 1/rate seconds."
  [streams k rate node-count]
  (paced (gaps (streams (keyword (str (name k) "-gaps"))) (/ 1.0 rate))
         (payments (streams k) node-count)))

(defn- targets
  "The nodes for a kill or pause target spec, chosen here so that the nemesis
  doesn't draw them. Specs other than :one and :all go to the nemesis as they are."
  [r nodes spec]
  (case spec
    :one [(pick r nodes)]
    :all (vec nodes)
    spec))

(defn partitions
  "Endless pairs of a partition that isolates one node and its healing. The grudge
  is computed here, not from :one in the nemesis."
  [r nodes]
  (mapcat (fn [node]
            [{:type  :info
              :f     :start-partition
              :value (n/complete-grudge [[node] (vec (remove #{node} nodes))])}
             {:type :info, :f :stop-partition, :value nil}])
          (repeatedly #(pick r nodes))))

(defn db-faults
  "Endless kill, start, pause and resume ops, as jepsen.nemesis.combined's db
  generators make them: each op comes from kill/start or pause/resume, chosen
  uniformly, and each of those alternates between its fault and its recovery."
  [r nodes {:keys [kill? pause? kill-targets pause-targets]}]
  (let [modes (cond-> [] pause? (conj :pause) kill? (conj :kill))]
    (when (seq modes)
      ((fn step [faulted]
         (lazy-seq
           (let [mode (pick r modes)
                 op   (if (faulted mode)
                        (case mode
                          :kill  {:type :info, :f :start, :value :all}
                          :pause {:type :info, :f :resume, :value :all})
                        (case mode
                          :kill  {:type  :info
                                  :f     :kill
                                  :value (targets r nodes (pick r kill-targets))}
                          :pause {:type  :info
                                  :f     :pause
                                  :value (targets r nodes (pick r pause-targets))}))]
             (cons op (step (if (faulted mode) (disj faulted mode) (conj faulted mode)))))))
       #{}))))

(defn disconnects
  "Endless pairs of a :disconnect of one random channel's peers and a :reconnect."
  [r nodes]
  (let [edges (vec (topology/edges nodes))]
    (mapcat (fn [edge]
              [{:type :info, :f :disconnect, :value edge}
               {:type :info, :f :reconnect}])
            (repeatedly #(pick r edges)))))

(defn faults
  "The fault generator for the test options `opts`: one paced stream per nemesis
  package in use, combined as nc/compose-packages combines them. Gaps average
  :nemesis-interval seconds. Nil without faults."
  [streams opts]
  (let [faults (set (:faults opts))
        nodes  (vec (:nodes opts))
        pace   (fn [k ops]
                 (paced (gaps (streams (keyword (str (name k) "-gaps")))
                              (:nemesis-interval opts))
                        ops))
        gens   (cond-> []
                 (faults :partition)
                 (conj (pace :partition (partitions (:partition streams) nodes)))

                 (some faults [:kill :pause])
                 (conj (pace :db (db-faults (:db streams) nodes
                                            {:kill?         (faults :kill)
                                             :pause?        (faults :pause)
                                             :kill-targets  (:kill-targets opts)
                                             :pause-targets [:one]})))

                 (faults :disconnect)
                 (conj (pace :disconnect (disconnects (:disconnect streams) nodes))))]
    (when (seq gens)
      (apply gen/any gens))))
