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
            [jepsen.fiber [checker :as fiber-checker]
                          [client :as client]
                          [db :as db]
                          [faults :as faults]
                          [invoice]
                          [ledger :as ledger]
                          [nonce :as nonce]
                          [scenarios :as scenarios]
                          [schedule :as schedule]
                          [topology :as topology]]
            [jepsen.fiber.faults.packet :as packet])
  (:gen-class))

(def supported-faults
  "Faults this suite can inject, one namespace each under jepsen.fiber.faults.
  Jepsen's :clock fault changes the host clock, which Docker containers share, so
  clock skew needs a per-process approach (libfaketime) instead."
  faults/supported)

(defn payments
  "Payments for the whole fault window, drawn from the run's seed (see
  jepsen.fiber.schedule): 0.01-0.2 CKB each, about :rate a second, of the kinds
  in --pay-kinds (keysend, invoice: jepsen.fiber.invoice). With --close,
  every channel is asked to close cooperatively halfway through, and payments go on
  against the closing channels, from a stream of their own."
  [opts streams]
  (let [pays (fn [k] (schedule/paced-payments streams k (:rate opts) (count (:nodes opts))
                                              (:pay-kinds opts [:keysend])))]
    (if (:close opts)
      (gen/phases (gen/time-limit (/ (:time-limit opts) 2) (pays :pay))
                  (gen/once {:f :close})
                  (pays :pay-after-close))
      (pays :pay))))

(defn fault-window
  "The payments and the faults, each for :time-limit seconds.

  Each gets a time limit of its own. A single gen/time-limit around both ends the
  window early: while every client is busy, the next operation is the nemesis's,
  and when that one falls after the limit, gen/time-limit takes the whole window
  as finished, payments included. A gap between faults can be up to 100 s, so a
  run with --nemesis-interval 30 and --time-limit 90 could stop after a second."
  [opts streams]
  (let [limit #(some->> % (gen/time-limit (:time-limit opts)))]
    (gen/nemesis (limit (faults/generator streams opts))
                 (limit (payments opts streams)))))

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
  (let [db      (db/db)
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
            :nemesis              (:nemesis pkg)
            ; Shared state between the topology setup, the clients and the checker.
            :pubkeys              (atom {})
            :payments             (atom [])
            :initial-channels     (atom nil)
            :nonserializable-keys [:pubkeys :payments :initial-channels]
            :checker              (checker/compose
                                    (cond-> {:stats      (checker/stats)
                                             :exceptions (checker/unhandled-exceptions)
                                             :perf       (checker/perf {:nemeses (:perf pkg)})
                                             :timeline   (timeline/html)}
                                      (not (:close opts)) (assoc :fiber (fiber-checker/checker))
                                      (:close opts)       (assoc :close (fiber-checker/close-checker))
                                      (:nonce-check opts) (assoc :nonce (nonce/checker))
                                      (:ledger opts)      (assoc :ledger (ledger/checker))))
            :generator            (gen/phases
                                    (fault-window opts streams)
                                    (gen/log "Healing all faults")
                                    (gen/nemesis (:final-generator pkg))
                                    (gen/log "Waiting for the network to converge")
                                    (gen/sleep (:quiesce opts))
                                    (when-not (:close opts) (gen/clients (gen/once {:f :probe})))
                                    (gen/clients (gen/once {:f :read})))})))

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
   [nil "--log-level FILTER" "RUST_LOG filter for fnn."
    :default  "info"]
   [nil "--kill-targets TARGETS" "Comma-separated kill targets: one, all."
    :default  [:one :all]
    :parse-fn keywords]
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
   [nil "--nonce-check" (str "Check MuSig2 nonce use. Needs an fnn built with the nonce-oracle"
                             " feature (scripts/build-fnn-nonce-oracle.sh).")]
   [nil "--close" (str "Halfway through the faults, ask every channel to close cooperatively, and"
                       " check that all of them end Closed instead of the usual invariants.")]
   [nil "--ledger" (str "Check each node's balance change against the payments it sent, received"
                        " and forwarded. Tags every payment with its attempt id and lists every"
                        " node's payments in the final read.")]])

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
