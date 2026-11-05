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
    "c-03"  {:priority :p0
             :plan     (str "A-B-C, multi-hop payments. B is down longer than the TLC expiry. The"
                            " upstream node eventually recovers the funds, and the payment is Failed.")
             :args     ["--faults" "outage" "--outage-targets" "middle" "--outage-duration" "120-180"
                        "--outage-jump" "34200" "--time-limit" "120" "--rate" "10"
                        "--tlc-expiry-delta" "9600000" "--final-tlc-expiry-delta" "9600000"
                        "--tlc-expiry-limit" "33660000" "--quiesce" "330" "--onchain"
                        "--settle-timeout" "900" "--expiry-check" "--force-close-ok"]
             :differs  (str "Three nodes in a line; payments go between all three, keysend only,"
                            " about 10 a second. Fiber adds a random privacy delay of 4 hours or"
                            " more to every hop's TLC expiry (rand_tlc_expiry_delta in graph.rs), so"
                            " no outage within a run outlasts a TLC; the clocks move instead (open"
                            " question 2: a clock jump, not a debug build). Every channel forwards"
                            " with the release minimum delta of 160 minutes, and payments ask for a"
                            " 160-minute final delta and a TLC expiry limit that pins the random"
                            " delay at 4 hours, so a payment's TLCs expire 6 h 40 min to 9 h 20 min"
                            " after it is sent. One outage takes n2 down for 120-180 s, 30 s into"
                            " the run, as soon as a neighbour holds a TLC in flight to it (it waits"
                            " up to 30 s more); while"
                            " n2 is down, every fnn's clock and the CKB node's jump 9.5 hours"
                            " forward, and n2 restarts with the new time. The"
                            " neighbours force-close the channels whose TLCs expired, so closed"
                            " channels are expected and --force-close-ok excuses them (this is not"
                            " open question 1). A node sees that its counterparty force-closed a"
                            " channel only in a check every 300 s (CHECK_CHANNELS_SHUTDOWN_INTERVAL),"
                            " so the quiesce lasts 330 s, for the restarted n2 to see the closes. After settling, --expiry-check wants every payment"
                            " Success or Failed and every payer's Failed TLC back on chain;"
                            " :unknown when the outage caught no payment in flight.")}
    "c-04"  {:priority :p0
             :plan     (str "4-6 node mesh, mixed payments. Random nodes restarted again and"
                            " again. Once all recover, the success rate is back to baseline.")
             :args     ["--nodes" "n1,n2,n3,n4,n5,n6" "--topology" "mesh" "--faults" "kill"
                        "--kill-targets" "one" "--nemesis-interval" "30" "--time-limit" "300"
                        "--pay-kinds" "keysend,invoice" "--availability-window" "60"
                        "--quiesce" "60" "--probe-routes"]
             :differs  (str "Six nodes, each with channels to three others (the ring and its"
                            " diameters), as in c-06. A kill (SIGKILL) takes one node drawn"
                            " uniformly; the gap between a kill and the restart, and between a"
                            " restart and the next kill, is exponential with a 30 s mean, capped"
                            " at 100 s. Baseline and recovery are 60 s of payments each, before"
                            " the first kill and after healing and --quiesce; the recovery's"
                            " success rate may be at most --availability-tolerance (5) points"
                            " below the baseline's. Each payment is a keysend or an invoice"
                            " payment, drawn uniformly, in all three windows. A channel that ends"
                            " force-closed fails the run; add --force-close-ok to excuse it (open"
                            " question 1).")}
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
                            " peer-response deadline.")}
    "c-14"  {:priority :p2
             :plan     "A-B, payments and invoices; a node's clock jumps forward or back"
             :args     ["--faults" "clock" "--clock-targets" "one,all" "--clock-jump" "60-1800"
                        "--pay-kinds" "keysend,invoice" "--nemesis-interval" "30" "--time-limit" "300"]
             :differs  (str "Three nodes in a line rather than A-B; half the payments keysend, half"
                            " invoices. Only fnn's clock moves, with libfaketime; the CKB devnet"
                            " keeps real time. One node's clock, or every node's by the same offset,"
                            " jumps 1-30 minutes forward or back and returns after an exponential"
                            " 30 s on average, capped at 100 s: well short of a TLC's expiry (hours)"
                            " and of an invoice's (1 h, so one made on a node 30 minutes ahead is"
                            " still unexpired on a node 30 minutes behind), so a force close or a"
                            " stuck payment is a failure.")}))

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
