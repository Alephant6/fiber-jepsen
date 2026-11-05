(ns jepsen.fiber.faults.packet
  "The packet fault: tc netem delay, and optionally loss, on traffic to some
  hosts, through jepsen.net/shape! and jepsen.nemesis.combined's packet nemesis.

  A target spec is one of:
    :one    a random fnn node: its traffic to the other nodes, and theirs to it;
    :all    every fnn node's traffic to every other node;
    :chain  every fnn node's traffic to the CKB node (not the replies).
  Only traffic between hosts is shaped; the control node's RPC calls are not.

  shape! replaces each node's root qdisc, so one shaping is active at a time:
  each :start-packet replaces the last one, and :stop-packet removes it. A fault
  that shapes traffic differently should add a target spec or option here rather
  than call shape! itself, or the two will undo each other."
  (:require [clojure.string :as str]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber.schedule :as schedule])
  (:import (java.util SplittableRandom)))

(def target-specs
  #{:one :all :chain})

(def chain-host
  "The CKB node's host name in docker/docker-compose.yml."
  "chain")

(defn parse-delay
  "\"500-3000\" -> [500 3000]; \"800\" -> [800 800]. Milliseconds."
  [s]
  (let [[lo hi] (map parse-long (str/split s #"-" 2))]
    [lo (or hi lo)]))

(defn behaviors
  "netem behaviors for jepsen.net/shape!: `delay-ms` with a jitter of a tenth of
  it, and `loss` percent of packets dropped when above zero."
  [delay-ms loss]
  (cond-> {:delay {:time         (str delay-ms "ms")
                   :jitter       (str (max 1 (quot delay-ms 10)) "ms")
                   :correlation  "25%"
                   :distribution :normal}}
    (pos? loss) (assoc :loss {:percent (str loss "%"), :correlation "25%"})))

(defn ops
  "Endless pairs of a :start-packet, with its targets and behaviors drawn from
  `r`, and a :stop-packet. Options: :packet-targets (specs to choose from),
  :packet-delay ([min max] ms, drawn uniformly) and :packet-loss (percent)."
  [^SplittableRandom r opts]
  (let [nodes   (vec (:nodes opts))
        specs   (vec (:packet-targets opts [:one :all]))
        [lo hi] (:packet-delay opts [500 3000])
        loss    (:packet-loss opts 0)]
    (mapcat (fn [[targets delay-ms]]
              [{:type :info, :f :start-packet, :value [targets (behaviors delay-ms loss)]}
               {:type :info, :f :stop-packet, :value nil}])
            (repeatedly
              (fn []
                (let [targets (case (schedule/pick r specs)
                                :one   [(schedule/pick r nodes)]
                                :all   nodes
                                :chain [chain-host])]
                  [targets (+ lo (.nextInt r (int (inc (- hi lo)))))]))))))

(def fault
  {:faults  #{:packet}
   :stream  :packet
   :package nc/packet-package
   :ops     ops
   :fired   {:packet #{:start-packet}}})
