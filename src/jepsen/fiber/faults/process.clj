(ns jepsen.fiber.faults.process
  "The kill and pause faults: SIGKILL (or, with --kill-signal term, SIGTERM) and
  restart, SIGSTOP and SIGCONT, on fnn processes (jepsen.fiber.db), through
  jepsen.nemesis.combined's db package."
  (:require [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber [schedule :as schedule]
                          [topology :as topology]]))

(def kill-signals
  "--kill-signal values: how jepsen.fiber.db stops fnn on a :kill op."
  #{:kill :term})

(def target-specs
  "--kill-targets values. Anything else would reach jepsen.nemesis.combined's
  db-nodes, which passes an unknown spec through as the node list, so every kill
  op would throw and the run would have no kills."
  #{:one :all :middle})

(defn middle-nodes
  "The nodes that forward payments: those on more than one channel of the test's
  topology (n2 in line), in the order of :nodes."
  [opts]
  (let [degree (frequencies (apply concat (topology/edges opts)))]
    (vec (filter #(< 1 (get degree % 0)) (:nodes opts)))))

(defn- targets
  "The nodes for a kill or pause target spec, chosen here so that the nemesis
  doesn't draw them: :one, :all, or :middle (one of the middle-nodes, at random).
  Other specs go to the nemesis as they are."
  [r nodes middles spec]
  (case spec
    :one    [(schedule/pick r nodes)]
    :all    (vec nodes)
    :middle [(schedule/pick r middles)]
    spec))

(defn- fault-op
  [r nodes middles mode kill-targets pause-targets]
  (case mode
    :kill  {:type  :info
            :f     :kill
            :value (targets r nodes middles (schedule/pick r kill-targets))}
    :pause {:type  :info
            :f     :pause
            :value (targets r nodes middles (schedule/pick r pause-targets))}))

(defn- recovery-op
  [mode]
  (case mode
    :kill  {:type :info, :f :start, :value :all}
    :pause {:type :info, :f :resume, :value :all}))

(defn ops
  "Endless kill, start, pause and resume ops, as jepsen.nemesis.combined's db
  generators make them: each op comes from kill/start or pause/resume, chosen
  uniformly, and each of those alternates between its fault and its recovery.
  So with both kill and pause, a pause lasts until pause/resume is chosen
  again, two gaps on average and often more, and kills and pauses overlap.

  With `paired?` (--process-duration), each fault is followed at once by its
  recovery instead: a kill or a pause, chosen uniformly, then its start or
  resume, so that `gaps` can set how long each lasts."
  [r nodes {:keys [kill? pause? kill-targets pause-targets middles paired?]}]
  (let [modes (cond-> [] pause? (conj :pause) kill? (conj :kill))]
    (cond
      (empty? modes)
      nil

      paired?
      (mapcat (fn [mode]
                [(fault-op r nodes middles mode kill-targets pause-targets)
                 (recovery-op mode)])
              (repeatedly #(schedule/pick r modes)))

      :else
      ((fn step [faulted]
         (lazy-seq
           (let [mode (schedule/pick r modes)
                 op   (if (faulted mode)
                        (recovery-op mode)
                        (fault-op r nodes middles mode kill-targets pause-targets))]
             (cons op (step (if (faulted mode) (disj faulted mode) (conj faulted mode)))))))
       #{}))))

(defn gaps
  "The gaps in ns between this fault's ops. Without :process-duration, every
  fault's gaps. With it ([min max] seconds), the ops pair up (see `ops`), each
  kill or pause lasts a whole number of seconds drawn from a stream of its own,
  and the gaps after each recovery come from the usual gap stream.

  Where the ops alternate between a fault and its recovery, no gap after a
  recovery is shorter than :nemesis-min-gap seconds: with :process-duration,
  and without it when only kills or only pauses are in use. With both and no
  duration, kills and pauses overlap (see `ops`), no gap is one fault's own,
  and the minimum does nothing."
  [streams opts]
  (let [usual (schedule/gaps (schedule/gaps-stream streams :db) (:nemesis-interval opts))
        modes (count (filter (set (:faults opts)) [:kill :pause]))]
    (if-let [[lo hi] (:process-duration opts)]
      (let [^java.util.SplittableRandom r (schedule/stream streams :process-duration)]
        (schedule/after-recovery-at-least
          (:nemesis-min-gap opts)
          (interleave (repeatedly #(* 1000000000 (+ lo (.nextInt r (int (inc (- hi lo)))))))
                      usual)))
      (cond->> usual
        (= 1 modes) (schedule/after-recovery-at-least (:nemesis-min-gap opts))))))

(defn term-exits
  "Every SIGTERM exit in `history` (--kill-signal term): one map per node and
  :kill completion, with the op's :time."
  [history]
  (for [op            history
        :when         (and (= :kill (:f op)) (map? (:value op)))
        [node result] (:value op)
        :when         (map? result)]
    (assoc result :node node, :time (:time op))))

(defn exit-checker
  "With --kill-signal term: valid when fnn exited within the grace period
  (--term-grace) after every SIGTERM, invalid when it had to be killed, and
  :unknown when no SIGTERM was sent. Reports the exit times."
  [grace-s]
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [exits  (term-exits history)
            times  (sort (keep :exit-ms exits))
            killed (vec (for [e exits :when (:killed-after-ms e)]
                          (select-keys e [:node :time :killed-after-ms])))]
        {:valid?        (cond (empty? exits)  :unknown
                              (seq killed)    false
                              :else           true)
         :term-grace-s  grace-s
         :terms         (count exits)
         :exited        (count times)
         :exit-ms       (when (seq times)
                          {:median (nth times (quot (count times) 2))
                           :max    (last times)})
         :slowest       (vec (take 3 (sort-by (comp - :exit-ms)
                                              (map #(select-keys % [:node :time :exit-ms])
                                                   (filter :exit-ms exits)))))
         :not-graceful  killed
         ; fnn exited, or was killed, without logging that it got the SIGTERM.
         :unlogged      (vec (for [e exits :when (false? (:logged? e))]
                               (select-keys e [:node :time])))}))))

(defn- not-running?
  "Whether fnn's ps state `st` from jepsen.fiber.db/signal! says there was no
  fnn to signal: none at all, or one that had exited but wasn't reaped yet."
  [st]
  (or (nil? st) (= :not-running st) (and (string? st) (str/starts-with? st "Z"))))

(defn effect
  "Whether a completed op took effect (see jepsen.fiber.faults). A :pause
  completion maps each node to fnn's ps state after SIGSTOP: it took effect
  when every running fnn is stopped (T), is :skipped when none was running
  (killed by another fault), and otherwise names the nodes it didn't stop.
  This is the check the pause fault lacked when it stopped nothing (before
  fiber-jepsen #10). Kills take effect unless they crash."
  [op]
  (when (and (= :pause (:f op)) (map? (:value op)))
    (let [running     (remove (comp not-running? val) (:value op))
          not-stopped (remove (fn [[_ st]] (and (string? st) (str/starts-with? st "T"))) running)]
      (cond (empty? running)    :skipped
            (seq not-stopped)   (str "fnn not stopped: "
                                     (str/join ", " (for [[node st] (sort-by key not-stopped)]
                                                      (str node " " st))))
            :else               nil))))

(def fault
  {:faults  #{:kill :pause}
   :stream  :db
   :package nc/db-package
   :ops     (fn [r opts]
              (let [faults  (set (:faults opts))
                    middles (middle-nodes opts)]
                (when (and (faults :kill) (some #{:middle} (:kill-targets opts)) (empty? middles))
                  (throw (ex-info (str "--kill-targets middle: no node in topology "
                                       (name (:topology opts :line)) " is on more than one channel")
                                  {:type :no-middle-node, :nodes (:nodes opts)})))
                (ops r (vec (:nodes opts))
                     {:kill?         (faults :kill)
                      :pause?        (faults :pause)
                      :kill-targets  (:kill-targets opts)
                      :pause-targets [:one]
                      :middles       middles
                      :paired?       (some? (:process-duration opts))})))
   :gaps    gaps
   :checker (fn [opts]
              (when (and (some #{:kill} (:faults opts)) (= :term (:kill-signal opts)))
                {:term-exits (exit-checker (:term-grace opts 30))}))
   :fired   {:kill #{:kill}, :pause #{:pause}}
   :effect  effect
   ; A pause is a local signal: one that stops nothing means the fault is broken.
   :strict? true})
