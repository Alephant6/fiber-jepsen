(ns jepsen.fiber.scenarios
  "Scenarios of the chaos-test plan (quake's gist \"Fiber 混沌测试用例\", C-01a to
  C-16) that this suite runs, by name, each as the arguments that run it.
  `--scenario NAME` puts a scenario's arguments right after the option, so
  arguments after it override them:

    scripts/run.sh --scenario c-01b --time-limit 60"
  (:require [cheshire.core :as json]
            [clojure.string :as str]))

(def scenarios
  "Name -> {:priority  the plan's priority
            :plan      the plan's fault, abridged
            :args      the arguments that run it here
            :differs   where this suite departs from the plan
            :compose   optional: the docker/docker-compose.NAME.yml file the
                       scenario needs, which scripts/run.sh adds with --compose}
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
                        "--time-limit" "300" "--ledger"]
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
                        "--quiesce" "60" "--probe-routes" "--ledger"]
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
    "c-06"  {:priority :p0
             :plan     (str "Mesh, multi-hop payments. A partition isolates half the nodes for"
                            " minutes, then heals. Routing works after healing, payments converge.")
             :args     ["--nodes" "n1,n2,n3,n4,n5,n6" "--topology" "mesh" "--faults" "partition"
                        "--partition-targets" "half" "--partition-duration" "120-300"
                        "--nemesis-interval" "60" "--time-limit" "600" "--quiesce" "60"
                        "--probe-routes" "--ledger"]
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
             :args     ["--faults" "pause" "--nemesis-interval" "30" "--time-limit" "300" "--ledger"]
             :differs  (str "Three nodes in a line rather than A-B. A pause lasts an exponential"
                            " 30 s on average, capped at 100 s, so some cross V2's 30 s"
                            " peer-response deadline.")}
    "c-09"  {:priority :p0
             :plan     (str "Any topology; channels opened and closed continuously; CKB disconnected"
                            " for seconds to minutes. Channels end Ready or Closed, never stuck in"
                            " between.")
             :args     ["--faults" "chain-partition" "--nemesis-interval" "40"
                        "--channel-ops" "open,close,force-close" "--channel-interval" "15"
                        "--time-limit" "300"]
             :differs  (str "Three nodes in a line. The fnn nodes' traffic with the CKB node is"
                            " dropped both ways, so requests time out, for one node or for all,"
                            " alternating with healing; each phase lasts an exponential 40 s on"
                            " average, capped at 100 s. Every 15 s on average a client opens,"
                            " cooperatively closes or force-closes a channel on an edge, keeping one"
                            " ready channel per edge. After the quiet period the test waits up to"
                            " 360 s for both ends of every channel to settle, since Fiber checks for"
                            " a counterparty's force close every 300 s.")}
    "c-10"  {:priority :p0
             :plan     (str "Any topology; continuous payments plus force closes; CKB delay of 1-10 s"
                            " with jitter. On-chain transactions confirm and balances reconcile.")
             :args     ["--faults" "packet" "--packet-targets" "chain" "--packet-delay" "1000-10000"
                        "--nemesis-interval" "30" "--channel-ops" "open,force-close"
                        "--channel-interval" "20" "--onchain" "--time-limit" "300"]
             :differs  (str "Three nodes in a line. The delay applies to the nodes' traffic to the"
                            " CKB node, not the replies, with a tenth of it as jitter, and alternates"
                            " with healing, each phase an exponential 30 s on average. Every 20 s on"
                            " average a client opens a channel or force-closes one on an edge,"
                            " keeping one ready channel per edge. After the final read every channel"
                            " still open is closed and --onchain checks each party's receipt and"
                            " each wallet. A TLC still pending on a force-closed channel settles"
                            " only once it expires, hours later in a release build, so that"
                            " channel's verdict is unknown rather than valid.")}
    "c-12"  {:priority :p1
             :plan     (str "A-B-C under high load; limit a node's CPU or memory with cgroups;"
                            " no OOM, no disconnect loop, recovers once the limit is lifted")
             :args     ["--faults" "cpu,memory" "--resource-targets" "one,all" "--rate" "20"
                        "--nemesis-interval" "30" "--time-limit" "300"
                        "--log-level" "info,fnn::fiber::network=debug" "--ledger"]
             :differs  (str "Limits are Docker's (docker update), on the node's whole container:"
                            " fnn and sshd. Each limit draws CPU (0.1-0.5 CPUs) or memory"
                            " (128-256 MiB, no swap) and one node or all of them, and lasts an"
                            " exponential 30 s on average, capped at 100 s. Lifting sets the limits"
                            " to the host's size, since Docker can't remove one. A disconnect loop"
                            " is a pair of nodes disconnecting 3 times or more within a limit and"
                            " the minute after it, from fnn's debug log.")}
    "c-13"  {:priority :p1
             :plan     (str "A-B under steady payments; fill the disk, then free it; the node"
                            " reports an error or stops, recovers once space is freed, loses no state")
             :args     ["--faults" "disk" "--rate" "20" "--nemesis-interval" "30" "--time-limit" "300"
                        "--ledger"]
             :differs  (str "Three nodes in a line rather than A-B. Only fnn's store (RocksDB) is"
                            " on the filled filesystem, a 256 MiB tmpfs per node; its log is not,"
                            " so the run can read what fnn said. Each fill takes one node at"
                            " random and lasts an exponential 30 s on average, capped at 100 s."
                            " RocksDB preallocates its write-ahead log, so a running fnn can go on"
                            " writing on a full disk; half the fills also crash fnn and start it"
                            " again on the full disk."
                            " If fnn isn't healthy 10 s after the space is freed, the harness"
                            " restarts it, as an operator would, and records that it had to.")}
    "c-14"  {:priority :p2
             :plan     "A-B, payments and invoices; a node's clock jumps forward or back"
             :args     ["--faults" "clock" "--clock-targets" "one,all" "--clock-jump" "60-1800"
                        "--pay-kinds" "keysend,invoice" "--nemesis-interval" "30" "--time-limit" "300"
                        "--ledger"]
             :differs  (str "Three nodes in a line rather than A-B; half the payments keysend, half"
                            " invoices. Only fnn's clock moves, with libfaketime; the CKB devnet"
                            " keeps real time. One node's clock, or every node's by the same offset,"
                            " jumps 1-30 minutes forward or back and returns after an exponential"
                            " 30 s on average, capped at 100 s: well short of a TLC's expiry (hours)"
                            " and of an invoice's (1 h, so one made on a node 30 minutes ahead is"
                            " still unexpired on a node 30 minutes behind), so a force close or a"
                            " stuck payment is a failure.")}
    "c-15"  {:priority :p1
             :plan     (str "CCH + LND, cross-chain swaps; kill CCH or disconnect LND at random."
                            " Every order ends completed or refunded, and both sides' funds agree")
             :compose  "cch"
             :args     ["--cch" "--faults" "cch-kill,lnd-disconnect" "--nemesis-interval" "20"
                        "--time-limit" "300" "--swap-rate" "0.5" "--rate" "2" "--concurrency" "6"
                        "--quiesce" "60"]
             :differs  (str "Needs the hosts of docker/docker-compose.cch.yml (scripts/run.sh"
                            " --compose cch). CCH runs as a separate service in front of n3, as in"
                            " Fiber's cross-chain-hub-separate e2e test; the CKB user is n1, on two"
                            " wrapped-BTC channels with n3, and the BTC user is lnd-bob, on one"
                            " channel with the hub's lnd-ingrid. Keysend payments over the line go"
                            " on alongside. An LND disconnect is an iptables cut of either CCH's link"
                            " to its LND or the link between the two LNDs. A kill is SIGKILL.")}
    "c-16"  {:priority :p1
             :plan     (str "Mesh; long mixed load; a random combination of the faults above, for"
                            " hours. Every assertion passes, no panic.")
             :args     ["--nodes" "n1,n2,n3,n4,n5,n6" "--topology" "mesh"
                        "--faults" "kill,pause,partition,disconnect,packet,chain-partition,cpu,memory,clock"
                        "--kill-targets" "one" "--process-duration" "5-20"
                        "--partition-targets" "one,half" "--partition-duration" "5-15"
                        "--packet-targets" "one,all,chain" "--clock-targets" "one,all"
                        "--resource-targets" "one" "--memory-limit" "160-320"
                        "--pay-kinds" "keysend,invoice" "--nemesis-interval" "30" "--time-limit" "1200"
                        "--availability-window" "60" "--quiesce" "60" "--probe-routes" "--ledger"
                        "--log-level" "info,fnn::fiber::network=debug"]
             :differs  (str "The short form: 20 minutes of faults, not hours (the long form and"
                            " where it runs wait for open questions 4 and 5). Six nodes, each with"
                            " channels to three others (the ring and its diameters), as in c-04 and"
                            " c-06; keysend and invoice payments, drawn uniformly. Nine faults, each"
                            " from its own stream of the seed, so they overlap at random: a kill"
                            " (SIGKILL) or a pause of one node for 5-20 s; a partition of one node or"
                            " of a random half for 5-15 s; a disconnect of a channel's peers; packet"
                            " delay of 0.5-3 s for one node, all, or traffic to CKB; a cut from the CKB"
                            " node; a CPU (0.1-0.5 CPUs) or memory (160-320 MiB) limit on one node;"
                            " and a clock jump of 1-30 minutes either way on one node or all. The gaps"
                            " between faults, and how long the others last, average 30 s, capped at"
                            " 100 s. Kills, pauses and partitions stay under fnn's 30 s peer deadline,"
                            " so that the run tests what open question 1 doesn't decide; a partition"
                            " under 30 s can still close a channel, since TCP's retransmissions back"
                            " off. The memory floor is above the 120 MiB of anonymous memory fnn"
                            " reached on this mesh (ae6f7d34), so an OOM kill means fnn grew. Not in"
                            " the mix: the outage fault (its clock jump is C-03's), disk and power"
                            " (each puts the store on a filesystem of its own), and c-15's CCH and"
                            " LND hosts. A closed channel fails the run. For the plan's longer"
                            " faults, add --process-duration 30-120 --partition-duration 30-120 and"
                            " --force-close-ok-after 30, which excuses only the closes that a fault"
                            " over 30 s explains (open question 1 answered \"expected\").")}))

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

(defn select
  "The names of the scenarios `spec` asks for: `all`, a priority (`p0`, `p1`,
  `p2`), or comma-separated names and priorities. Throws for an unknown name."
  [spec]
  (let [parts (remove str/blank? (map str/trim (str/split (or spec "all") #",")))]
    (->> (for [p parts]
           (cond (= "all" p)          (keys scenarios)
                 (re-matches #"p\d" p) (for [[n s] scenarios
                                            :when (= (keyword p) (:priority s))]
                                        n)
                 :else                (do (scenario-args p) [p])))
         (apply concat)
         distinct
         sort
         vec)))

(defn matrix
  "The scenarios `spec` asks for (see `select`), as a GitHub Actions matrix:
  [{:name :priority :args}], where :args is the string scripts/run.sh takes."
  [spec]
  (vec (for [n (select spec)
             :let [{:keys [priority compose]} (get scenarios n)]]
         {:name     n
          :priority (name priority)
          :args     (str/join " " (concat (when compose ["--compose" compose])
                                          ["--scenario" n]))})))

(defn -main
  "Prints `matrix` of the first argument (default all) as JSON, for
  .github/workflows/matrix.yml."
  [& [spec]]
  (println (json/generate-string (matrix spec)))
  (flush))
