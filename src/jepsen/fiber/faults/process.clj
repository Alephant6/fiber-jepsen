(ns jepsen.fiber.faults.process
  "The kill and pause faults: SIGKILL (or, with --kill-signal term, SIGTERM) and
  restart, SIGSTOP and SIGCONT, on fnn processes (jepsen.fiber.db), through
  jepsen.nemesis.combined's db package."
  (:require [jepsen.checker :as checker]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber [schedule :as schedule]
                          [topology :as topology]]))

(def kill-signals
  "--kill-signal values: how jepsen.fiber.db stops fnn on a :kill op."
  #{:kill :term})

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

(defn ops
  "Endless kill, start, pause and resume ops, as jepsen.nemesis.combined's db
  generators make them: each op comes from kill/start or pause/resume, chosen
  uniformly, and each of those alternates between its fault and its recovery."
  [r nodes {:keys [kill? pause? kill-targets pause-targets middles]}]
  (let [modes (cond-> [] pause? (conj :pause) kill? (conj :kill))]
    (when (seq modes)
      ((fn step [faulted]
         (lazy-seq
           (let [mode (schedule/pick r modes)
                 op   (if (faulted mode)
                        (case mode
                          :kill  {:type :info, :f :start, :value :all}
                          :pause {:type :info, :f :resume, :value :all})
                        (case mode
                          :kill  {:type  :info
                                  :f     :kill
                                  :value (targets r nodes middles (schedule/pick r kill-targets))}
                          :pause {:type  :info
                                  :f     :pause
                                  :value (targets r nodes middles (schedule/pick r pause-targets))}))]
             (cons op (step (if (faulted mode) (disj faulted mode) (conj faulted mode)))))))
       #{}))))

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
         :not-graceful  killed}))))

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
                      :middles       middles})))
   :checker (fn [opts]
              (when (and (some #{:kill} (:faults opts)) (= :term (:kill-signal opts)))
                {:term-exits (exit-checker (:term-grace opts 30))}))})
