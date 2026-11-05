(ns jepsen.fiber
  "Entry point. Runs concurrent keysend payments over a graph of channels (by
  default a line, n1 - n2 - n3) while injecting faults, heals everything, waits
  for the network to converge, then checks the invariants in
  jepsen.fiber.checker. The faults are in jepsen.fiber.faults, the named
  scenarios of the chaos-test plan in jepsen.fiber.scenarios."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.fiber [cch :as cch]
                          [channels :as channels]
                          [checker :as fiber-checker]
                          [client :as client]
                          [closes :as closes]
                          [db :as db]
                          [faults :as faults]
                          [health :as health]
                          [invoice]
                          [ledger :as ledger]
                          [nonce :as nonce]
                          [onchain :as onchain]
                          [scenarios :as scenarios]
                          [schedule :as schedule]
                          [swap :as swap]
                          [topology :as topology]]
            [jepsen.fiber.faults [chain-partition :as chain-partition]
                                 [clock :as clock]
                                 [lnd-disconnect :as lnd-disconnect]
                                 [packet :as packet]
                                 [partition :as partition]
                                 [process :as process]])
  (:gen-class))

(def supported-faults
  "Faults this suite can inject, one namespace each under jepsen.fiber.faults.
  Jepsen's own clock nemesis changes the host clock, which Docker containers
  share, so :clock here moves fnn's clock alone, with libfaketime."
  faults/supported)

(defn payments
  "Payments for the whole fault window, drawn from the run's seed (see
  jepsen.fiber.schedule): 0.01-0.2 CKB each, about :rate a second, of the kinds
  in --pay-kinds (keysend, invoice: jepsen.fiber.invoice). With --close,
  every channel is asked to close cooperatively halfway through, and payments go on
  against the closing channels, from a stream of their own."
  [opts streams]
  (let [pays (fn [k] (schedule/paced-payments streams k (:rate opts) (count (:nodes opts))
                                              (:pay-kinds opts [:keysend])))
        base (if (:close opts)
               (gen/phases (gen/time-limit (/ (:time-limit opts) 2) (pays :pay))
                           (gen/once {:f :close})
                           (pays :pay-after-close))
               (pays :pay))]
    ; With --cch, cross-chain swaps (jepsen.fiber.swap) go alongside.
    (if (:cch opts)
      (gen/any base (swap/generator opts streams))
      base)))

(defn availability?
  "Whether the test measures the success rate before the faults and after
  healing (--availability-window). Not with --close: every channel is closed
  by the end."
  [opts]
  (and (pos? (:availability-window opts 0)) (not (:close opts))))

(defn availability-window
  "With --availability-window, payments without faults for that many seconds,
  at --rate and of the --pay-kinds, from a stream of their own (`k`), each op
  marked with :window `window` for jepsen.fiber.checker/availability-checker.
  Nil without the option. The streams are new, so the fault window's payments
  for a seed don't change."
  [opts streams k window]
  (when (availability? opts)
    (->> (schedule/paced-payments streams k (:rate opts) (count (:nodes opts))
                                  (:pay-kinds opts [:keysend]))
         (gen/map #(assoc % :window window))
         (gen/time-limit (:availability-window opts))
         gen/clients)))

(defn client-ops
  "The clients' operations during the fault window: the payments and, with
  --channel-ops, the channel workload (jepsen.fiber.channels)."
  [opts streams]
  (let [pays (payments opts streams)]
    (if-let [chans (channels/generator streams opts)]
      (gen/any pays chans)
      pays)))

(defn fault-window
  "The client operations and the faults, each for :time-limit seconds.

  Each gets a time limit of its own. A single gen/time-limit around both ends the
  window early: while every client is busy, the next operation is the nemesis's,
  and when that one falls after the limit, gen/time-limit takes the whole window
  as finished, payments included. A gap between faults can be up to 100 s, so a
  run with --nemesis-interval 30 and --time-limit 90 could stop after a second."
  [opts streams]
  (let [limit #(some->> % (gen/time-limit (:time-limit opts)))]
    (gen/nemesis (limit (faults/generator streams opts))
                 (limit (client-ops opts streams)))))

(defn nemesis-package
  "The nemesis package for the requested faults (jepsen.fiber.faults/package).
  fiber-test uses its nemesis, final generator and perf, but takes the faults
  themselves from jepsen.fiber.faults/generator, so that the seed fixes them."
  [db opts]
  (faults/package db opts))

(defn fiber-test
  "The test for `opts`. Without --seed it picks a seed, which jepsen.log shows in
  the test map (:seed): pass it as --seed to run the same payments and faults again."
  [opts]
  (swap/check-opts! opts)
  (let [db      (cond-> (db/db) (:cch opts) cch/db)
        faults  (set (:faults opts))
        pkg     (nemesis-package db opts)
        seed    (or (:seed opts) (schedule/new-seed))
        streams (schedule/streams seed)]
    (merge tests/noop-test
           opts
           {:name                 (str "fiber-" (if (seq faults)
                                                  (str/join "," (map name (sort faults)))
                                                  "none"))
            :seed                 seed
            :os                   os/noop
            :db                   db
            :client               (client/client)
            :nemesis              (health/compose (:nemesis pkg))
            ; Shared state between the topology setup, the clients and the checker.
            :pubkeys              (atom {})
            :fnn-env              (faults/fnn-env opts)
            :payments             (atom [])
            :initial-channels     (atom nil)
            ; With --cch: the swaps that got a payment hash, and the swap
            ; environment's starting state (jepsen.fiber.cch).
            :swaps                (atom [])
            :swap-env             (atom nil)
            :nonserializable-keys [:pubkeys :payments :initial-channels :swaps :swap-env]
            :checker              (checker/compose
                                    (cond-> (merge (faults/checkers opts)
                                                   {:stats      (checker/stats)
                                                    :exceptions (checker/unhandled-exceptions)
                                                    :perf       (checker/perf {:nemeses (:perf pkg)})
                                                    :timeline   (timeline/html)
                                                    :health     (health/checker)})
                                      (not (:close opts)) (assoc :fiber (if (:force-close-ok opts)
                                                                          (closes/checker)
                                                                          (fiber-checker/checker)))
                                      (:close opts)       (assoc :close (fiber-checker/close-checker))
                                      (:nonce-check opts) (assoc :nonce (nonce/checker))
                                      (:ledger opts)      (assoc :ledger (ledger/checker))
                                      (:onchain opts)     (assoc :onchain (onchain/checker))
                                      (seq (:channel-ops opts)) (assoc :channels (channels/checker))
                                      (:cch opts)         (assoc :swap (swap/checker))
                                      (availability? opts)
                                      (assoc :availability (fiber-checker/availability-checker
                                                             (:availability-tolerance opts 5)))))
            ; With --availability-window, a baseline window of payments before the
            ; faults and a recovery window after the network has converged.
            :generator            (gen/phases
                                    (when (:onchain opts) (gen/clients (gen/once {:f :chain-baseline})))
                                    (availability-window opts streams :pay-baseline :baseline)
                                    (fault-window opts streams)
                                    (gen/log "Healing all faults")
                                    (gen/nemesis (:final-generator pkg))
                                    (gen/log "Waiting for the network to converge")
                                    (gen/sleep (:quiesce opts))
                                    (when (seq (:channel-ops opts))
                                      (gen/clients (gen/once {:f :settle-channels})))
                                    (availability-window opts streams :pay-recovery :recovery)
                                    (swap/final-generator opts)
                                    (when-not (:close opts) (gen/clients (gen/once {:f :probe})))
                                    ; Assertion 7 (jepsen.fiber.health): is every fnn there and working?
                                    (gen/nemesis (gen/once health/final-sample))
                                    (gen/clients (gen/once {:f :read}))
                                    (when (:onchain opts)
                                      (gen/phases (gen/log "Closing every channel and waiting for it to settle on chain")
                                                  (gen/clients (gen/once {:f :settle})))))})))

(defn- keywords
  [s]
  (mapv keyword (str/split s #",")))

(def cli-opts
  [[nil "--scenario NAME" (str "Run a named scenario of the chaos-test plan: its arguments go in"
                               " place of this option, so arguments after it override them. One of: "
                               (str/join ", " (keys scenarios/scenarios)) ".")]
   [nil "--faults FAULTS" (str "Comma-separated faults to inject: "
                               (str/join ", " (sort (map name supported-faults)))
                               "; or none, for a run without faults (a smoke test of the harness).")
    :default  [:kill]
    :parse-fn (fn [s] (if (= "none" s) [] (keywords s)))
    :validate [#(every? supported-faults %) (str "Must be a subset of " supported-faults)]]
   [nil "--topology NAME" (str "Channel graph: " (str/join ", " (map name (keys topology/topologies))) ".")
    :default  :line
    :parse-fn keyword
    :validate [topology/topologies (str "Must be one of " (keys topology/topologies))]]
   [nil "--rate HZ" "Approximate number of payments per second."
    :default  5
    :parse-fn parse-double]
   [nil "--pay-kinds KINDS" (str "Comma-separated payment kinds, each payment's drawn uniformly from"
                                 " them (list one twice to draw it twice as often): keysend, invoice.")
    :default  [:keysend]
    :parse-fn keywords
    :validate [#(every? (methods client/send-payment!) %)
               (str "Each must be a payment kind: " (str/join ", " (map name (keys (methods client/send-payment!)))))]]
   [nil "--nemesis-interval SECONDS" "Seconds between fault operations."
    :default  15
    :parse-fn parse-long]
   [nil "--quiesce SECONDS" "Seconds to wait after healing before the final read."
    :default  30
    :parse-fn parse-long]
   [nil "--availability-window SECONDS"
    (str "Send payments without faults for this many seconds before the faults (the baseline)"
         " and again after --quiesce (the recovery), and check that the success rate has come"
         " back to the baseline's. 0 turns it off. Not with --close.")
    :default  0
    :parse-fn parse-long
    :validate [#(and % (<= 0 %)) "Must be 0 or more"]]
   [nil "--availability-tolerance POINTS"
    (str "With --availability-window, how many percentage points the recovery's success rate"
         " may fall below the baseline's.")
    :default  5.0
    :parse-fn parse-double
    :validate [#(and % (<= 0 % 100)) "Must be 0 to 100"]]
   [nil "--log-level FILTER" "RUST_LOG filter for fnn."
    :default  "info"]
   [nil "--kill-targets TARGETS" (str "Comma-separated kill targets: one, all, middle (a node on more"
                                      " than one channel, which forwards payments: n2 in line).")
    :default  [:one :all]
    :parse-fn keywords]
   [nil "--kill-signal SIGNAL" (str "How a kill stops fnn: kill (SIGKILL, a crash) or term (SIGTERM, a"
                                    " graceful shutdown, checked to finish within --term-grace).")
    :default  :kill
    :parse-fn keyword
    :validate [process/kill-signals (str "Must be one of " process/kill-signals)]]
   [nil "--term-grace SECONDS" (str "With --kill-signal term, seconds fnn has to exit after SIGTERM"
                                    " before it gets SIGKILL and the run fails.")
    :default  30
    :parse-fn parse-long
    :validate [#(and % (pos? %)) "Must be positive"]]
   [nil "--packet-targets TARGETS" (str "With --faults packet, comma-separated target specs to choose"
                                        " from: one, all, chain (the CKB node).")
    :default  [:one :all]
    :parse-fn keywords
    :validate [#(every? packet/target-specs %) (str "Must be a subset of " packet/target-specs)]]
   [nil "--packet-delay MS" "With --faults packet, delay in ms, or a range MIN-MAX drawn per fault."
    :default  [500 3000]
    :parse-fn packet/parse-delay
    :validate [(fn [[lo hi]] (and lo hi (<= 0 lo hi))) "Must be MS or MIN-MAX, 0 <= MIN <= MAX"]]
   [nil "--packet-loss PERCENT" "With --faults packet, percent of packets to drop as well."
    :default  0
    :parse-fn parse-long
    :validate [#(and % (<= 0 % 100)) "Must be 0 to 100"]]
   [nil "--partition-targets TARGETS" (str "With --faults partition, comma-separated target specs to"
                                           " choose from: one (one node), half (a random half of the"
                                           " nodes).")
    :default  [:one]
    :parse-fn keywords
    :validate [#(every? partition/target-specs %) (str "Must be a subset of " partition/target-specs)]]
   [nil "--partition-duration SECONDS" (str "With --faults partition, how long each partition lasts,"
                                            " or a range MIN-MAX drawn per partition. Default: until"
                                            " the next fault op, like other faults (at most 100 s).")
    :parse-fn partition/parse-duration
    :validate [(fn [[lo hi]] (and lo hi (< 0 lo) (<= lo hi))) "Must be S or MIN-MAX, 0 < MIN <= MAX"]]
   [nil "--chain-partition-targets TARGETS" (str "With --faults chain-partition, comma-separated"
                                                 " target specs to choose from: one (a random fnn"
                                                 " node), all.")
    :default  [:one :all]
    :parse-fn keywords
    :validate [#(every? chain-partition/target-specs %)
               (str "Must be a subset of " chain-partition/target-specs)]]
   [nil "--chain-partition-mode MODE" (str "With --faults chain-partition: drop (requests to the CKB"
                                           " node time out) or reject (TCP reset, they fail at once).")
    :default  :drop
    :parse-fn keyword
    :validate [chain-partition/modes (str "Must be one of " chain-partition/modes)]]
   [nil "--channel-ops OPS" (str "Comma-separated channel operations to run with the payments: open,"
                                 " close (cooperative) and force-close, on the topology's edges"
                                 " (jepsen.fiber.channels). Also checks that every channel ends ready"
                                 " or closed.")
    :default  []
    :parse-fn keywords
    :validate [#(every? channels/kinds %) (str "Must be a subset of " (keys channels/kinds))]]
   [nil "--channel-interval SECONDS" "With --channel-ops, mean seconds between channel operations."
    :default  20
    :parse-fn parse-double]
   [nil "--channel-funding CKB" "With --channel-ops, CKB the opener puts into each new channel."
    :default  500
    :parse-fn parse-long]
   [nil "--channel-keep N" "With --channel-ops, closes leave at least this many ready channels on each edge."
    :default  1
    :parse-fn parse-long]
   [nil "--channel-settle-timeout SECONDS" (str "With --channel-ops, how long to wait after the quiet period"
                                                " for every channel to be ready or closed on both ends. Fiber"
                                                " checks for a counterparty's force close every 300 s.")
    :default  360
    :parse-fn parse-long]
   [nil "--disconnect-flash MS" (str "With --faults disconnect, make each fault a flash: disconnect a pair"
                                     " and connect it again after MS milliseconds, or after a time drawn"
                                     " from MIN-MAX (C-05: 100-5000). Without it, a pair stays"
                                     " disconnected until the next fault op.")
    :parse-fn packet/parse-delay
    :validate [(fn [[lo hi]] (and lo hi (<= 0 lo hi))) "Must be MS or MIN-MAX, 0 <= MIN <= MAX"]]
   [nil "--clock-targets TARGETS" (str "With --faults clock, comma-separated target specs to choose"
                                       " from: one (a random node's clock jumps), all (every node's,"
                                       " by the same offset).")
    :default  [:one]
    :parse-fn keywords
    :validate [#(every? clock/target-specs %) (str "Must be a subset of " clock/target-specs)]]
   [nil "--clock-jump SECONDS" (str "With --faults clock, how far fnn's clock jumps, or a range MIN-MAX"
                                    " drawn per jump; forward or back at random.")
    :default  [60 1800]
    :parse-fn clock/parse-range
    :validate [(fn [[lo hi]] (and lo hi (<= 1 lo hi))) "Must be SECONDS or MIN-MAX, 1 <= MIN <= MAX"]]
   [nil "--nonce-check" (str "Check MuSig2 nonce use. Needs an fnn built with the nonce-oracle"
                             " feature (scripts/build-fnn-nonce-oracle.sh).")]
   [nil "--close" (str "Halfway through the faults, ask every channel to close cooperatively, and"
                       " check that all of them end Closed instead of the usual invariants.")]
   [nil "--ledger" (str "Check each node's balance change against the payments it sent, received"
                        " and forwarded. Tags every payment with its attempt id and lists every"
                        " node's payments in the final read.")]
   [nil "--probe-routes" (str "After healing, also pay from every node to every node it has no"
                              " channel with, so that the probe tests multi-hop routing.")]
   [nil "--force-close-ok" (str "Don't fail the run for channels that ended closed, nor for what"
                                " they explain (open question 1 on the chaos-test plan: a pause or"
                                " partition over 30 s force-closes channels). They are reported.")]
   [nil "--onchain" (str "After the final read, close every channel and check on chain that each"
                         " party received its last balance plus its reserve, less fees, and that"
                         " every node's wallet changed by what its channels' transactions explain.")]
   [nil "--settle-timeout SECONDS" "With --onchain, how long to wait for the closes to settle on chain."
    :default  300
    :parse-fn parse-long]
   ; Cross-chain swaps (jepsen.fiber.swap, jepsen.fiber.cch; C-15).
   [nil "--cch" (str "Also run cross-chain swaps through a standalone cross-chain hub (CCH) and two"
                     " LNDs, and check that every order ends completed or refunded. Needs the hosts"
                     " of docker/docker-compose.cch.yml: scripts/run.sh --compose cch.")]
   [nil "--swap-rate HZ" "With --cch, approximate number of swaps per second."
    :default  0.5
    :parse-fn parse-double
    :validate [pos? "Must be positive"]]
   [nil "--swap-amount SATS" "With --cch, each swap's amount in sats, or a range MIN-MAX drawn per swap."
    :default  [1000 20000]
    :parse-fn swap/parse-range
    :validate [(fn [[lo hi]] (and lo hi (<= 1 lo hi))) "Must be SATS or MIN-MAX, 1 <= MIN <= MAX"]]
   [nil "--swap-kinds KINDS" (str "With --cch, comma-separated swap kinds to choose from: send-btc"
                                  " (CKB to BTC), receive-btc (BTC to CKB).")
    :default  [:send-btc :receive-btc]
    :parse-fn keywords
    :validate [#(every? swap/kinds %) (str "Must be a subset of " swap/kinds)]]
   [nil "--swap-settle-timeout SECONDS" (str "With --cch, how long the final swap read waits for every"
                                             " order to finish.")
    :default  180
    :parse-fn parse-long]
   [nil "--lnd-disconnect-targets TARGETS" (str "With --faults lnd-disconnect, comma-separated links to"
                                                " choose from: cch (CCH to the hub's LND), peer (the"
                                                " hub's LND to the BTC user's).")
    :default  [:cch :peer]
    :parse-fn keywords
    :validate [#(every? lnd-disconnect/target-specs %)
               (str "Must be a subset of " lnd-disconnect/target-specs)]]
   [nil "--lnd-disconnect-mode MODE" (str "With --faults lnd-disconnect: drop (calls hang until they"
                                          " time out) or reject (TCP reset, they fail at once).")
    :default  :drop
    :parse-fn keyword
    :validate [lnd-disconnect/modes (str "Must be one of " lnd-disconnect/modes)]]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  fiber-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            (try (scenarios/expand args)
                 (catch clojure.lang.ExceptionInfo e
                   (if (= :unknown-scenario (:type (ex-data e)))
                     (do (binding [*out* *err*] (println (ex-message e)))
                         (System/exit 254))
                     (throw e))))))
