(ns jepsen.fiber
  "Entry point. Runs concurrent keysend payments over a line of channels
  (n1 - n2 - n3) while injecting faults, heals everything, waits for the network
  to converge, then checks the invariants in jepsen.fiber.checker."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber [checker :as fiber-checker]
                          [client :as client]
                          [db :as db]])
  (:gen-class))

(def supported-faults
  "Faults this suite can inject. Jepsen's :clock fault changes the host clock,
  which Docker containers share, so clock skew needs a per-process approach
  (libfaketime) instead."
  #{:kill :pause :partition})

(defn pay-op
  "A payment of 0.01-0.2 CKB. The client picks the destination."
  []
  {:f :pay, :value {:amount (* 1000000 (inc (rand-int 20)))}})

(defn nemesis-package
  "Composes only the nemesis packages for the requested faults. The full
  nc/nemesis-package also sets up file-corruption tooling (it downloads bitflip
  onto every node) even when that fault is disabled."
  [db opts]
  (let [faults (set (:faults opts))
        popts  {:db        db
                :nodes     (:nodes opts)
                :faults    faults
                :interval  (:nemesis-interval opts)
                :kill      {:targets (:kill-targets opts)}
                :pause     {:targets [:one]}
                :partition {:targets [:one]}}]
    (nc/compose-packages
      (cond-> []
        (faults :partition)             (conj (nc/partition-package popts))
        (some faults [:kill :pause])    (conj (nc/db-package popts))))))

(defn fiber-test
  [opts]
  (let [db     (db/db)
        faults (set (:faults opts))
        pkg    (nemesis-package db opts)]
    (merge tests/noop-test
           opts
           {:name                 (str "fiber-" (str/join "," (map name (sort faults))))
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
                                    {:stats      (checker/stats)
                                     :exceptions (checker/unhandled-exceptions)
                                     :perf       (checker/perf {:nemeses (:perf pkg)})
                                     :timeline   (timeline/html)
                                     :fiber      (fiber-checker/checker)})
            :generator            (gen/phases
                                    (->> pay-op
                                         (gen/stagger (/ (:rate opts)))
                                         (gen/nemesis (:generator pkg))
                                         (gen/time-limit (:time-limit opts)))
                                    (gen/log "Healing all faults")
                                    (gen/nemesis (:final-generator pkg))
                                    (gen/log "Waiting for the network to converge")
                                    (gen/sleep (:quiesce opts))
                                    (gen/clients (gen/once {:f :read})))})))

(def cli-opts
  [[nil "--faults FAULTS" "Comma-separated faults to inject: kill, pause, partition."
    :default  [:kill]
    :parse-fn (fn [s] (mapv keyword (str/split s #",")))
    :validate [#(every? supported-faults %) (str "Must be a subset of " supported-faults)]]
   [nil "--rate HZ" "Approximate number of payments per second."
    :default  5
    :parse-fn parse-double]
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
    :parse-fn (fn [s] (mapv keyword (str/split s #",")))]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  fiber-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
