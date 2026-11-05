(ns jepsen.fiber.scenarios
  "Scenarios of the chaos-test plan (quake's gist \"Fiber 混沌测试用例\", C-01a to
  C-16) that this suite runs, by name, each as the arguments that run it.
  `--scenario NAME` puts a scenario's arguments right after the option, so
  arguments after it override them:

    scripts/run.sh --scenario c-01b --time-limit 60"
  (:require [clojure.string :as str]))

(def scenarios
  "Name -> {:priority  the plan's priority
            :plan      the plan's fault, abridged
            :args      the arguments that run it here
            :differs   where this suite departs from the plan}
  A scenario is listed once the suite covers it. Keep the names in order."
  (sorted-map
    "c-01b" {:priority :p0
             :plan     "kill -9 (SIGKILL) A or B at random every 30 to 120 s, then restart"
             :args     ["--faults" "kill" "--kill-targets" "one" "--nemesis-interval" "75"
                        "--time-limit" "300"]
             :differs  (str "Three nodes in a line rather than A-B. The gap between a kill and"
                            " the restart, and between restarts and kills, is exponential with a"
                            " 75 s mean, capped at 100 s, rather than uniform over 30-120 s.")}
    "c-12"  {:priority :p1
             :plan     (str "A-B-C under high load; limit a node's CPU or memory with cgroups;"
                            " no OOM, no disconnect loop, recovers once the limit is lifted")
             :args     ["--faults" "cpu,memory" "--resource-targets" "one,all" "--rate" "20"
                        "--nemesis-interval" "30" "--time-limit" "300"
                        "--log-level" "info,fnn::fiber::network=debug"]
             :differs  (str "Limits are Docker's (docker update), on the node's whole container:"
                            " fnn and sshd. Each limit draws CPU (0.1-0.5 CPUs) or memory"
                            " (64-256 MiB, no swap) and one node or all of them, and lasts an"
                            " exponential 30 s on average, capped at 100 s. Lifting sets the limits"
                            " to the host's size, since Docker can't remove one. A disconnect loop"
                            " is a pair of nodes disconnecting 3 times or more within a limit and"
                            " the minute after it, from fnn's debug log.")}
    "c-08"  {:priority :p1
             :plan     "SIGSTOP a node for tens of seconds, then resume"
             :args     ["--faults" "pause" "--nemesis-interval" "30" "--time-limit" "300"]
             :differs  (str "Three nodes in a line rather than A-B. A pause lasts an exponential"
                            " 30 s on average, capped at 100 s, so some cross V2's 30 s"
                            " peer-response deadline.")}))

(defn scenario-args
  "The arguments of scenario `name`. Throws for an unknown name."
  [name]
  (or (:args (get scenarios name))
      (throw (ex-info (str "Unknown scenario " (pr-str name) "; known: "
                           (str/join ", " (keys scenarios)))
                      {:type :unknown-scenario, :name name}))))

(defn expand
  "Command-line `args` with each --scenario NAME (or --scenario=NAME) followed by
  that scenario's arguments. The option stays, so the test map records the name."
  [args]
  (loop [[a & more] args, out []]
    (cond (nil? a)
          out

          (= "--scenario" a)
          (let [[n & more] more]
            (recur more (into (conj out a n) (scenario-args n))))

          (str/starts-with? a "--scenario=")
          (recur more (into (conj out a) (scenario-args (subs a (count "--scenario=")))))

          :else
          (recur more (conj out a)))))
