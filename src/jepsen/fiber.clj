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
                          [expiry :as expiry]
                          [faults :as faults]
                          [health :as health]
                          [invoice]
                          [ledger :as ledger]
                          [mpp :as mpp]
                          [nonce :as nonce]
                          [onchain :as onchain]
                          [scenarios :as scenarios]
                          [schedule :as schedule]
                          [swap :as swap]
                          [topology :as topology]
                          [udt :as udt]]
            [jepsen.fiber.faults [chain-partition :as chain-partition]
                                 [clock :as clock]
                                 [lnd-disconnect :as lnd-disconnect]
                                 [old-state :as old-state]
                                 [outage :as outage]
                                 [packet :as packet]
                                 [partition :as partition]
                                 [power :as power]
                                 [process :as process]
                                 [resources :as resources]])
  (:gen-class))

(def supported-faults
  "Faults this suite can inject, one namespace each under jepsen.fiber.faults.
  Jepsen's own clock nemesis changes the host clock, which Docker containers
  share, so :clock here moves fnn's clock alone, with libfaketime."
  faults/supported)

(defn payments
  "Payments for the whole fault window, drawn from the run's seed (see
  jepsen.fiber.schedule): 0.01-0.2 CKB each, about :rate a second, of the kinds
  in --pay-kinds (keysend, invoice: jepsen.fiber.invoice, mpp:
  jepsen.fiber.mpp, whose amounts are --mpp-amount's). With --close,
  every channel is asked to close cooperatively halfway through, and payments go on
  against the closing channels, from a stream of their own."
  [opts streams]
  (let [pays (fn [k] (schedule/paced-payments streams k (:rate opts) (count (:nodes opts))
                                              (:pay-kinds opts [:keysend])
                                              (mpp/amount-ranges opts)))
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
                                  (:pay-kinds opts [:keysend]) (mpp/amount-ranges opts))
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
  (udt/check-opts! opts)
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
            ; jepsen.net/iptables, but a partition's healing leaves other faults' rules.
            :net                  partition/net
            :db                   db
            :client               (client/client)
            :nemesis              (health/compose (:nemesis pkg))
            ; Shared state between the topology setup, the clients and the checker.
            :pubkeys              (atom {})
            :fnn-env              (merge (faults/fnn-env opts) (topology/fnn-env opts))
            :payments             (atom [])
            ; With --pay-kinds mpp: every MPP invoice, for :read-invoices.
            :invoices             (atom [])
            :initial-channels     (atom nil)
            ; With --cch: the swaps that got a payment hash, and the swap
            ; environment's starting state (jepsen.fiber.cch).
            :swaps                (atom [])
            :swap-env             (atom nil)
            :nonserializable-keys [:pubkeys :payments :invoices :initial-channels :swaps :swap-env]
            :checker              (checker/compose
                                    (cond-> (merge (faults/checkers opts)
                                                   {:stats      (checker/stats)
                                                    :exceptions (checker/unhandled-exceptions)
                                                    :perf       (checker/perf {:nemeses (:perf pkg)})
                                                    :timeline   (timeline/html)
                                                    :health     (health/checker)})
                                      (seq (faults/in-use opts)) (assoc :faults (faults/fired-checker opts))
                                      (not (:close opts)) (assoc :fiber (cond-> (cond (:force-close-ok opts)
                                                                                        (closes/checker)

                                                                                        (:force-close-ok-after opts)
                                                                                        (closes/checker (:force-close-ok-after opts))

                                                                                        :else
                                                                                        (fiber-checker/checker))
                                                                                ; What a restored node forgot.
                                                                                (some #{:old-state} (:faults opts))
                                                                                old-state/excused))
                                      (:close opts)       (assoc :close (fiber-checker/close-checker))
                                      (:nonce-check opts) (assoc :nonce (nonce/checker))
                                      (:ledger opts)      (assoc :ledger (ledger/checker))
                                      (mpp/enabled? opts) (assoc :mpp (mpp/checker))
                                      (:onchain opts)     (assoc :onchain (onchain/checker))
                                      (seq (:channel-ops opts)) (assoc :channels (channels/checker))
                                      (:expiry-check opts) (assoc :expiry (expiry/checker))
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
                                    (when (mpp/enabled? opts)
                                      (gen/clients (gen/once {:f :read-invoices})))
                                    (when (:onchain opts)
                                      (gen/phases (gen/log "Closing every channel and waiting for it to settle on chain")
                                                  (gen/clients (gen/once {:f :settle}))))
                                    (when (:expiry-check opts)
                                      (gen/clients (gen/once {:f :final-payments}))))})))

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
   [nil "--asset ASSET" (str "What the channels hold and the payments move: ckb, or udt, the"
                             " devnet's simple UDT, which only n1-n3 hold (jepsen.fiber.udt). With"
                             " udt every channel opened at setup is funded with the token and every"
                             " payment, invoice and probe is in it.")
    :default  :ckb
    :parse-fn keyword
    :validate [#{:ckb :udt} "Must be ckb or udt"]]
   [nil "--rate HZ" "Approximate number of payments per second."
    :default  5
    :parse-fn parse-double]
   [nil "--pay-kinds KINDS" (str "Comma-separated payment kinds, each payment's drawn uniformly from"
                                 " them (list one twice to draw it twice as often): keysend, invoice,"
                                 " mpp (an invoice that allows multi-part payment, of --mpp-amount;"
                                 " checks that each ends all or nothing).")
    :default  [:keysend]
    :parse-fn keywords
    :validate [#(every? (methods client/send-payment!) %)
               (str "Each must be a payment kind: " (str/join ", " (map name (keys (methods client/send-payment!)))))]]
   [nil "--mpp-amount CKB" (str "With --pay-kinds mpp, each MPP payment's amount in CKB, or a range"
                                " MIN-MAX the amounts are spread over. Fiber splits a payment only"
                                " when no single route carries it, so choose amounts above any one"
                                " channel's balance.")
    :default  mpp/default-amount-ckb
    :parse-fn packet/parse-delay
    :validate [(fn [[lo hi]] (and lo hi (<= 1 lo hi))) "Must be CKB or MIN-MAX, 1 <= MIN <= MAX"]]
   [nil "--mpp-max-parts N" (str "With --pay-kinds mpp, the most parts a payment may be split into"
                                 " (send_payment's max_parts, 2-24). Default: Fiber's (12).")
    :parse-fn parse-long
    :validate [#(and % (<= 2 % 24)) "Must be 2 to 24"]]
   [nil "--acceptor-funding CKB" (str "CKB the acceptor of each channel opened at setup puts in, as"
                                      " the opener puts in 1000 (Fiber's"
                                      " auto_accept_channel_ckb_funding_amount, through"
                                      " FIBER_AUTO_ACCEPT_CHANNEL_CKB_FUNDING_AMOUNT). About 100 CKB"
                                      " of either side is reserve. Default: Fiber's, 100, all"
                                      " reserve, so the acceptor starts with nothing to send.")
    :parse-fn parse-long
    :validate [#(and % (<= 100 %)) "Must be 100 or more"]]
   [nil "--nemesis-interval SECONDS" "Seconds between fault operations."
    :default  15
    :parse-fn parse-long]
   [nil "--nemesis-min-gap SECONDS"
    (str "With --process-duration or --partition-duration, at least this many seconds between"
         " the end of a kill, pause or partition and the start of the fault's next one. The gaps"
         " average --nemesis-interval and can be a fraction of a second, so two short faults in"
         " a row can keep a node or a link away for as long as one long fault. 0 leaves the gaps"
         " as drawn. Each fault paces itself: two faults in one test can still follow each other"
         " closely.")
    :default  0
    :parse-fn parse-long
    :validate [#(and % (<= 0 %)) "Must be 0 or more"]]
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
    :parse-fn keywords
    :validate [#(every? process/target-specs %) (str "Must be a subset of " process/target-specs)]]
   [nil "--process-duration SECONDS" (str "With --faults kill or pause, how long each kill or pause"
                                          " lasts: a whole number of seconds, or a range MIN-MAX drawn"
                                          " per fault. Each is then followed at once by its restart or"
                                          " resume, and kills and pauses don't overlap. Default: until"
                                          " the stream's next op of the same kind, two gaps on average"
                                          " when both run.")
    :parse-fn partition/parse-duration
    :validate [(fn [[lo hi]] (and lo hi (< 0 lo) (<= lo hi))) "Must be S or MIN-MAX, 0 < MIN <= MAX"]]
   [nil "--kill-signal SIGNAL" (str "How a kill stops fnn: kill (SIGKILL, a crash) or term (SIGTERM, a"
                                    " graceful shutdown, checked to finish within --term-grace).")
    :default  :kill
    :parse-fn keyword
    :validate [process/kill-signals (str "Must be one of " process/kill-signals)]]
   [nil "--tlc-expiry-delta MS" (str "After opening the channels, set every channel's TLC expiry delta"
                                     " to MS milliseconds (update_channel) and wait until every node"
                                     " routes with it. A release build's minimum is 160 minutes"
                                     " (9600000).")
    :parse-fn parse-long
    :validate [#(and % (pos? %)) "Must be positive"]]
   [nil "--final-tlc-expiry-delta MS" (str "Payments ask for this final TLC expiry delta, in"
                                           " milliseconds (send_payment, and new_invoice for"
                                           " invoices). A release build's minimum is 160 minutes.")
    :parse-fn parse-long
    :validate [#(and % (pos? %)) "Must be positive"]]
   [nil "--tlc-expiry-limit MS" (str "Payments ask that no TLC expire more than MS milliseconds after"
                                     " it is sent (send_payment's tlc_expiry_limit). Fiber adds a"
                                     " random delay of 4 hours or more to every hop, so the limit must"
                                     " cover 4 hours plus the route's deltas; just over that, it pins"
                                     " the delay at 4 hours (C-03).")
    :parse-fn parse-long
    :validate [#(and % (pos? %)) "Must be positive"]]
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
   [nil "--outage-targets TARGETS" (str "With --faults outage, comma-separated target specs to choose"
                                        " from: middle (a node that forwards payments, n2 in line), one"
                                        " (any node).")
    :default  [:middle]
    :parse-fn keywords
    :validate [#(every? outage/target-specs %) (str "Must be a subset of " outage/target-specs)]]
   [nil "--outage-duration SECONDS" (str "With --faults outage, how long each outage keeps its node down,"
                                         " or a range MIN-MAX drawn per outage.")
    :default  [120 180]
    :parse-fn partition/parse-duration
    :validate [(fn [[lo hi]] (and lo hi (< 0 lo) (<= lo hi))) "Must be S or MIN-MAX, 0 < MIN <= MAX"]]
   [nil "--outage-after SECONDS" (str "With --faults outage, how long into the fault window the first"
                                      " outage waits, so that payments flow before it.")
    :default  30
    :parse-fn parse-long
    :validate [#(and % (<= 0 %)) "Must be 0 or more"]]
   [nil "--outage-await SECONDS" (str "With --faults outage, how long an outage waits for a neighbour"
                                      " of its node to hold a TLC in flight to it before it kills the"
                                      " node anyway.")
    :default  30
    :parse-fn parse-long
    :validate [#(and % (<= 0 %)) "Must be 0 or more"]]
   [nil "--outage-jump SECONDS" (str "With --faults outage, move every fnn's clock and the CKB node's"
                                     " forward by this much during each outage that caught a TLC in"
                                     " flight, so that the TLC expires (C-03). The clocks never jump"
                                     " back. 0: no jump.")
    :default  0
    :parse-fn parse-long
    :validate [#(and % (<= 0 %)) "Must be 0 or more"]]
   [nil "--resource-targets TARGETS" (str "With --faults cpu or memory, comma-separated target specs to"
                                          " choose from: one (a random node), all.")
    :default  [:one]
    :parse-fn keywords
    :validate [#(every? resources/target-specs %) (str "Must be a subset of " resources/target-specs)]]
   [nil "--cpu-limit CPUS" (str "With --faults cpu, the CPUs a limited node's container gets (a CFS"
                                " quota), or a range MIN-MAX drawn per fault.")
    :default  [0.1 0.5]
    :parse-fn resources/parse-range
    :validate [(fn [[lo hi]] (and lo hi (<= 0.01 lo hi))) "Must be CPUS or MIN-MAX, 0.01 <= MIN <= MAX"]]
   [nil "--memory-limit MIB" (str "With --faults memory, the memory a limited node's container gets"
                                  " (MiB, no swap), or a range MIN-MAX drawn per fault. The default"
                                  " starts above fnn's anonymous memory under c-12's load (at most"
                                  " 92 MiB on ae6f7d34), so an OOM kill means fnn grew, not that the"
                                  " limit was below what it always needs.")
    :default  [128 256]
    :parse-fn resources/parse-range
    :validate [(fn [[lo hi]] (and lo hi (<= 16 lo hi))) "Must be MIB or MIN-MAX, 16 <= MIN <= MAX"]]
   [nil "--power-targets TARGETS" (str "With --faults power, comma-separated target specs to choose"
                                       " from: one (a node), all (every node at once).")
    :default  [:one :all]
    :parse-fn keywords
    :validate [#(every? power/target-specs %) (str "Must be a subset of " power/target-specs)]]
   [nil "--store-sync-writes BOOL" (str "Sets fnn's fiber.store_sync_writes (Fiber #1703; earlier builds"
                                        " ignore it): true, every store write waits for the disk;"
                                        " false, it returns once in the page cache. Unset, fnn's"
                                        " default (false).")
    :parse-fn #(case % "true" true "false" false nil)
    :validate [boolean? "Must be true or false"]]
   [nil "--old-state-targets TARGETS" (str "With --faults old-state, comma-separated target specs to"
                                           " choose from: one (any node), middle (a node that forwards"
                                           " payments, n2 in line).")
    :default  [:one]
    :parse-fn keywords
    :validate [#(every? old-state/target-specs %) (str "Must be a subset of " old-state/target-specs)]]
   [nil "--old-state-restore MODE" (str "With --faults old-state, how the saved store comes back: raw"
                                        " (the store directory is replaced with the copy, as a disk"
                                        " or snapshot rollback would) or fiber (fnn --restore of the"
                                        " newest backup fnn made itself, which marks the restored"
                                        " channels Stale).")
    :default  :raw
    :parse-fn keyword
    :validate [old-state/restore-modes (str "Must be one of " old-state/restore-modes)]]
   [nil "--old-state-after SECONDS" (str "With --faults old-state, how long into the fault window the"
                                         " first save waits, so that payments flow before it.")
    :default  20
    :parse-fn parse-long
    :validate [#(and % (<= 0 %)) "Must be 0 or more"]]
   [nil "--old-state-hold SECONDS" (str "With --faults old-state, seconds from a save of a node's store"
                                        " to its restore, while payments move the node's channels on.")
    :default  40
    :parse-fn parse-long
    :validate [#(and % (pos? %)) "Must be positive"]]
   [nil "--old-state-count N" (str "With --faults old-state, how many save-and-restore pairs to run at"
                                   " most (by default, as many as the fault window holds; one comes"
                                   " every --old-state-hold + 100 s).")
    :parse-fn parse-long
    :validate [#(and % (pos? %)) "Must be positive"]]
   [nil "--old-state-force-close" (str "With --faults old-state, have the restored node force-close every"
                                       " channel it still has, once its peers have reconnected: with"
                                       " --old-state-restore raw it broadcasts the revoked commitments it"
                                       " holds, which its peers' watchtowers should punish.")]
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
   [nil "--force-close-ok-after SECONDS"
    (str "Like --force-close-ok, but only for a channel one of whose ends was killed or paused,"
         " or whose ends a partition cut apart, for longer than SECONDS during the run (fnn's"
         " peer deadline is 30 s): open question 1 answered \"expected\". A channel closed"
         " without such a fault still fails the run. --force-close-ok overrides it.")
    :parse-fn parse-long
    :validate [#(and % (pos? %)) "Must be positive"]]
   [nil "--onchain" (str "After the final read, close every channel and check on chain that each"
                         " party received its last balance plus its reserve, less fees, and that"
                         " every node's wallet changed by what its channels' transactions explain.")]
   [nil "--settle-timeout SECONDS" "With --onchain, how long to wait for the closes to settle on chain."
    :default  300
    :parse-fn parse-long]
   [nil "--expiry-check" (str "With --onchain (C-03): after the closes settle, read every payment"
                              " again and check that each is Success or Failed, and that a node got"
                              " back on chain every TLC of its own Failed payments that a closed"
                              " channel still held.")]
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
