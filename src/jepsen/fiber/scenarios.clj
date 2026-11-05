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
    "c-04"  {:priority :p0
             :plan     (str "4-6 node mesh, mixed payments. Random nodes restarted again and"
                            " again. Once all recover, the success rate is back to baseline.")
             :args     ["--nodes" "n1,n2,n3,n4,n5,n6" "--topology" "mesh" "--faults" "kill"
                        "--kill-targets" "one" "--nemesis-interval" "30" "--time-limit" "300"
                        "--availability-window" "60" "--quiesce" "60" "--probe-routes"]
             :differs  (str "Six nodes, each with channels to three others (the ring and its"
                            " diameters), as in c-06. A kill (SIGKILL) takes one node drawn"
                            " uniformly; the gap between a kill and the restart, and between a"
                            " restart and the next kill, is exponential with a 30 s mean, capped"
                            " at 100 s. Baseline and recovery are 60 s of payments each, before"
                            " the first kill and after healing and --quiesce; the recovery's"
                            " success rate may be at most --availability-tolerance (5) points"
                            " below the baseline's. Payments are keysends only, until"
                            " --pay-kinds lands. A channel that ends force-closed fails the run;"
                            " add --force-close-ok to excuse it (open question 1).")}
    "c-06"  {:priority :p0
             :plan     (str "Mesh, multi-hop payments. A partition isolates half the nodes for"
                            " minutes, then heals. Routing works after healing, payments converge.")
             :args     ["--nodes" "n1,n2,n3,n4,n5,n6" "--topology" "mesh" "--faults" "partition"
                        "--partition-targets" "half" "--partition-duration" "120-300"
                        "--nemesis-interval" "60" "--time-limit" "600" "--quiesce" "60"
                        "--probe-routes"]
             :differs  (str "Six nodes, each with channels to three others (the ring and its"
                            " diameters); 6 of the 15 node pairs are two hops apart, the rest"
                            " neighbours, and each payment goes to a node drawn uniformly. Each"
                            " partition cuts a random half from the other half for 120-300 s,"
                            " then heals for an exponential 60 s on average, capped at 100 s."
                            " Routing after healing is a payment between every two nodes without"
                            " a channel, on top of the usual probe of each channel. A partition"
                            " over 30 s force-closes the channels across it that had an update in"
                            " flight, which fails the run; add --force-close-ok to excuse closed"
                            " channels and what they explain (open question 1).")}
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
