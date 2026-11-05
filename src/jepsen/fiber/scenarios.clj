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
    "c-01a" {:priority :p0
             :plan     "kill (SIGTERM) A or B at random every 30 to 120 s, then restart"
             :args     ["--nodes" "n1,n2" "--faults" "kill" "--kill-signal" "term"
                        "--kill-targets" "one" "--nemesis-interval" "20" "--time-limit" "300"
                        "--availability-window" "30" "--ledger"]
             :differs  (str "A-B as in the plan: --nodes n1,n2 opens one channel (the cluster's"
                            " n3 stays idle). A kill comes every two gaps, the restart one gap"
                            " after it; gaps are exponential with a 20 s mean, capped at 100 s, so"
                            " kills come about every 40 s rather than uniformly every 30-120 s, and"
                            " most restarts come within 30 s, past which the peer force-closes the"
                            " channel (open question 1 to the team). fnn must exit within"
                            " --term-grace, 30 s.")}
    "c-01b" {:priority :p0
             :plan     "kill -9 (SIGKILL) A or B at random every 30 to 120 s, then restart"
             :args     ["--faults" "kill" "--kill-targets" "one" "--nemesis-interval" "75"
                        "--time-limit" "300"]
             :differs  (str "Three nodes in a line rather than A-B. The gap between a kill and"
                            " the restart, and between restarts and kills, is exponential with a"
                            " 75 s mean, capped at 100 s, rather than uniform over 30-120 s.")}
    "c-02a" {:priority :p0
             :plan     "kill (SIGTERM) the middle node B of A-B-C during multi-hop invoice payments, then restart"
             :args     ["--faults" "kill" "--kill-signal" "term" "--kill-targets" "middle"
                        "--pay-kinds" "invoice" "--nemesis-interval" "20" "--time-limit" "300"
                        "--availability-window" "30" "--ledger"]
             :differs  (str "Payments go between any two of the three nodes, so only those between"
                            " n1 and n3 cross the middle node, about a third. Kill and restart gaps"
                            " as in c-01a. fnn must exit within --term-grace, 30 s.")}
    "c-02b" {:priority :p0
             :plan     "kill -9 (SIGKILL) the middle node B of A-B-C during multi-hop invoice payments, then restart"
             :args     ["--faults" "kill" "--kill-signal" "kill" "--kill-targets" "middle"
                        "--pay-kinds" "invoice" "--nemesis-interval" "20" "--time-limit" "300"
                        "--availability-window" "30" "--ledger"]
             :differs  (str "As c-02a: about a third of the payments cross the middle node, and the"
                            " kill and restart gaps are those of c-01a.")}
    "c-05"  {:priority :p0
             :plan     "flash the network between A and B for 100 ms to 5 s at random, while they pay each other"
             :args     ["--nodes" "n1,n2" "--faults" "disconnect" "--disconnect-flash" "100-5000"
                        "--nemesis-interval" "8" "--time-limit" "300"
                        "--availability-window" "30" "--ledger"]
             :differs  (str "A-B as in the plan (--nodes n1,n2). A flash is disconnect_peer and,"
                            " after a time drawn from 100-5000 ms, connect_peer, not packet loss,"
                            " which TCP rides out. fnn often reconnects by itself after about 2 s,"
                            " before the drawn time is up. One flash every 8 s on average. n2"
                            " starts with nothing on the channel, so its payments fail until n1"
                            " has paid it.")}
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
