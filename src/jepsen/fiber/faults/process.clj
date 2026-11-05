(ns jepsen.fiber.faults.process
  "The kill and pause faults: SIGKILL and restart, SIGSTOP and SIGCONT, on fnn
  processes (jepsen.fiber.db), through jepsen.nemesis.combined's db package."
  (:require [jepsen.nemesis.combined :as nc]
            [jepsen.fiber.schedule :as schedule]))

(defn- targets
  "The nodes for a kill or pause target spec, chosen here so that the nemesis
  doesn't draw them. Specs other than :one and :all go to the nemesis as they are."
  [r nodes spec]
  (case spec
    :one [(schedule/pick r nodes)]
    :all (vec nodes)
    spec))

(defn ops
  "Endless kill, start, pause and resume ops, as jepsen.nemesis.combined's db
  generators make them: each op comes from kill/start or pause/resume, chosen
  uniformly, and each of those alternates between its fault and its recovery."
  [r nodes {:keys [kill? pause? kill-targets pause-targets]}]
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
                                  :value (targets r nodes (schedule/pick r kill-targets))}
                          :pause {:type  :info
                                  :f     :pause
                                  :value (targets r nodes (schedule/pick r pause-targets))}))]
             (cons op (step (if (faulted mode) (disj faulted mode) (conj faulted mode)))))))
       #{}))))

(def fault
  {:faults  #{:kill :pause}
   :stream  :db
   :package nc/db-package
   :ops     (fn [r opts]
              (let [faults (set (:faults opts))]
                (ops r (vec (:nodes opts))
                     {:kill?         (faults :kill)
                      :pause?        (faults :pause)
                      :kill-targets  (:kill-targets opts)
                      :pause-targets [:one]})))})
