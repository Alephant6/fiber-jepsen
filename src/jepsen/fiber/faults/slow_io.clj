(ns jepsen.fiber.faults.slow-io
  "Slow IO: a node's disk becomes slow for a while, and later is fast again. The
  node stays up and connected the whole time; only its store's reads and writes
  take longer.

  With this fault, jepsen.fiber.db puts each node's store on unreliablefs
  (jepsen.fiber.slowfs), a FUSE filesystem that can delay operations. A
  :start-slow-io op makes every write and fsync (--slow-io-ops) on its nodes'
  stores wait a fixed delay first, drawn per op from --slow-io-delay; a
  :stop-slow-io op ends that. Delayed operations are served one at a time, so
  under load the waits add up: fnn's store writes queue behind each other.

  Each op measures what it did. A start writes a block through the mount and
  syncs it, and records how long that took: at least the delay, or the fault
  didn't bite (the :faults check fails the run for that). A stop first records
  how fnn was doing while slow (a health sample: does node_info answer, and how
  long does new_invoice, a store write, take?), then ends the slowdown and
  writes the block again, which must be fast.

  The slow-io check is about the fault, not about fnn: it fails when an op
  threw, when a start didn't slow the store, when a store was still slow after
  a stop, or when a node's mount was gone; it reports each slow window with its
  delay, fnn's store-write time in it, and how the payments from or to the node
  ended. What fnn made of a slow disk shows in the usual checks: payments,
  channels, forced closes, the ledger, the health check."
  (:require [jepsen [checker :as checker]
                    [control :as c]
                    [nemesis :as n]]
            [jepsen.fiber [health :as health]
                          [schedule :as schedule]
                          [slowfs :as slowfs]]
            [jepsen.fiber.faults.process :as process])
  (:import (java.util SplittableRandom)))

(def target-specs
  #{:one :all :middle})

(def default-delay-ms
  "The range a slowdown's delay per operation is drawn from, in ms."
  [20 200])

(def default-ops
  [:write :fsync])

(def stop-probes
  "How many times a stop writes its probe block before it calls the store still
  slow: one fsync on a busy host can take longer than a small delay by itself."
  3)

(defn check-mounted!
  "Throws unless `node`'s store is on the unreliablefs mount."
  [node]
  (when-not (slowfs/mounted?)
    (throw (ex-info (str "--faults slow-io needs fnn's store on " node " to be on unreliablefs;"
                         " scripts/run.sh builds the node image with it when its arguments have"
                         " slow-io in --faults, or --compose slowio")
                    {:type :store-not-on-slowfs, :node node}))))

(defn start!
  "Slows the current node's store. Returns the delay and the probe's time
  under it."
  [{:keys [delay-ms ops]}]
  (slowfs/configure! {:delay-ms delay-ms, :ops ops})
  {:delay-ms delay-ms
   :probe-ms (slowfs/probe-ms)})

(defn- brief
  "The parts of a health sample a slow window's record keeps."
  [sample]
  (cond-> {:status (health/status sample)}
    (get-in sample [:rpc :ms])            (assoc :rpc-ms (get-in sample [:rpc :ms]))
    (get-in sample [:rpc :error])         (assoc :rpc-error (get-in sample [:rpc :error]))
    (get-in sample [:store-write :ms])    (assoc :store-write-ms (get-in sample [:store-write :ms]))
    (get-in sample [:store-write :error]) (assoc :store-write-error (get-in sample [:store-write :error]))))

(defn stop!
  "Records how fnn on `node` is doing on its slow store, ends the slowdown, and
  probes the store again: fast means a probe under the delay, within
  stop-probes tries."
  [test node {:keys [delay-ms]}]
  (if-not (slowfs/mounted?)
    {:delay-ms delay-ms, :mount-lost true}
    (let [slow   (health/sample! test node)
          _      (slowfs/configure! nil)
          probes (loop [n stop-probes, acc []]
                   (let [ms  (slowfs/probe-ms)
                         acc (conj acc ms)]
                     (if (or (< ms delay-ms) (= 1 n))
                       acc
                       (recur (dec n) acc))))]
      (cond-> {:delay-ms       delay-ms
               :while-slow     (brief slow)
               :probe-ms-after (peek probes)}
        (<= delay-ms (peek probes)) (assoc :still-slow true)))))

(defn slow-io-nemesis
  []
  (let [slowed (atom (sorted-map))]   ; node -> the slowdown it is under
    (reify
      n/Reflection
      (fs [_] #{:start-slow-io :stop-slow-io})

      n/Nemesis
      (setup! [this test]
        (c/on-nodes test (fn [_ node] (check-mounted! node)))
        this)

      (invoke! [_ test op]
        (case (:f op)
          :start-slow-io
          (let [slowdown (select-keys (:value op) [:delay-ms :ops])]
            (assoc op :value
                   (into (sorted-map)
                         (c/on-nodes test (:nodes (:value op))
                                     (fn [_ node]
                                       ; Before the change, so that a start that throws
                                       ; half way still gets its stop.
                                       (swap! slowed assoc node slowdown)
                                       (start! slowdown))))))

          :stop-slow-io
          (let [nodes @slowed]
            (assoc op :value
                   (into (sorted-map)
                         (when (seq nodes)
                           (c/on-nodes test (vec (keys nodes))
                                       (fn [test node]
                                         (let [r (stop! test node (nodes node))]
                                           (swap! slowed dissoc node)
                                           r)))))))))

      (teardown! [_ test]
        (c/on-nodes test (fn [_ _]
                           (when (slowfs/mounted?)
                             (slowfs/configure! nil))))))))

(defn package
  [opts]
  {:generator       nil
   :final-generator (when ((:faults opts) :slow-io) {:type :info, :f :stop-slow-io, :value nil})
   :nemesis         (slow-io-nemesis)
   :perf            #{{:name  "slow io"
                       :start #{:start-slow-io}
                       :stop  #{:stop-slow-io}
                       :color "#C9B1E9"}}})

(defn ops
  "Endless pairs of a :start-slow-io and a :stop-slow-io. Each start's nodes
  (:slow-io-targets: one node, a node that forwards payments, or all of them)
  and its delay per operation in ms (uniform over :slow-io-delay, [min max])
  are drawn from `r`; :slow-io-ops are the operations it delays."
  [^SplittableRandom r opts]
  (let [nodes   (vec (:nodes opts))
        specs   (vec (:slow-io-targets opts [:one]))
        middles (process/middle-nodes opts)
        [lo hi] (:slow-io-delay opts default-delay-ms)
        fs-ops  (vec (:slow-io-ops opts default-ops))]
    (when (and (some #{:middle} specs) (empty? middles))
      (throw (ex-info (str "--slow-io-targets middle: no node in topology "
                           (name (:topology opts :line)) " is on more than one channel")
                      {:type :no-middle-node, :nodes nodes})))
    (mapcat (fn [slowdown]
              [{:type :info, :f :start-slow-io, :value slowdown}
               {:type :info, :f :stop-slow-io, :value nil}])
            (repeatedly #(let [targets (case (schedule/pick r specs)
                                         :one    [(schedule/pick r nodes)]
                                         :middle [(schedule/pick r middles)]
                                         :all    nodes)]
                           {:nodes    targets
                            :delay-ms (+ lo (.nextInt r (int (inc (- hi lo)))))
                            :ops      fs-ops})))))

;; What the ops did

(defn- per-node
  "A :start-slow-io or :stop-slow-io completion's {node record}; nil for an
  invocation."
  [op]
  (let [v (:value op)]
    (when (and (map? v) (every? string? (keys v)))
      v)))

(defn slowed?
  "Whether a start's record shows the store slowed: the probe took at least the
  delay."
  [{:keys [delay-ms probe-ms]}]
  (boolean (and delay-ms probe-ms (<= delay-ms probe-ms))))

(defn effect
  "For the :faults check: nil when a completed :start-slow-io slowed every
  store it was to, else why not."
  [op]
  (when (= :start-slow-io (:f op))
    (let [recs (per-node op)
          bad  (remove (comp slowed? val) recs)]
      (cond (empty? recs) "no node was slowed"
            (seq bad)     (let [[node {:keys [delay-ms probe-ms]}] (first bad)]
                            (str "a write and fsync on " node "'s store took " probe-ms
                                 " ms under a delay of " delay-ms " ms"))))))

;; The checker

(defn- pays-of
  "How the payments from or to `node` that completed in [from, to] ended."
  [history node from to]
  (frequencies
    (for [op    history
          :when (and (= :pay (:f op)) (not= :invoke (:type op)) (<= from (:time op) to)
                     (or (= node (get-in op [:value :from])) (= node (get-in op [:value :to]))))]
      (:type op))))

(defn- spread
  "{:min :median :max} of numbers; nil for none."
  [xs]
  (when-let [xs (seq (sort xs))]
    {:min (first xs), :median (nth xs (quot (count xs) 2)), :max (last xs)}))

(defn check
  "The slow-io check on `history`; see the namespace doc. A window is one node's
  slowdown, from its start to the stop that ended it."
  [history]
  (let [started (atom {})   ; node -> its start's record and time
        events  (vec
                  (for [op    history
                        :when (#{:start-slow-io :stop-slow-io} (:f op))
                        [node rec] (per-node op)]
                    (if (= :start-slow-io (:f op))
                      (do (swap! started assoc node (assoc rec :time (:time op)))
                          {:start (assoc rec :node node, :time (:time op))})
                      (let [start (get @started node)]
                        (swap! started dissoc node)
                        {:window (cond-> (merge {:node     node
                                                 :time     (:time op)
                                                 :delay-ms (:delay-ms rec)
                                                 :payments (pays-of history node (:time start 0) (:time op))}
                                                (:while-slow rec)
                                                (select-keys rec [:probe-ms-after :still-slow :mount-lost]))
                                   (:probe-ms start) (assoc :probe-ms (:probe-ms start)))}))))
        starts   (keep :start events)
        windows  (vec (keep :window events))
        unslowed (vec (remove slowed? starts))
        still    (vec (filter :still-slow windows))
        lost     (vec (filter :mount-lost windows))
        crashed  (vec (for [op history
                            :when (and (#{:start-slow-io :stop-slow-io} (:f op)) (:exception op))]
                        (select-keys op [:f :time :error])))]
    {:valid?         (cond (seq crashed)    false
                           (seq unslowed)   false
                           (seq still)      false
                           (seq lost)       false
                           (empty? windows) :unknown
                           :else            true)
     :windows-count  (count windows)
     :slowed         (into (sorted-map) (frequencies (map :node windows)))
     :delay-ms       (spread (map :delay-ms windows))
     ; fnn on its slow store, sampled at the end of each window.
     :while-slow     (frequencies (keep :status windows))
     :store-write-ms (spread (keep :store-write-ms windows))
     :not-slowed     unslowed
     :still-slow     still
     :mount-lost     lost
     :nemesis-errors crashed
     :windows        windows}))

(defn checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (check history))))

(def fault
  {:faults  #{:slow-io}
   :stream  :slow-io
   :package package
   :ops     ops
   :checker (fn [_opts] {:slow-io (checker)})
   :fired   {:slow-io #{:start-slow-io}}
   :effect  effect
   :strict? true})
