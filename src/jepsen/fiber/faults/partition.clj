(ns jepsen.fiber.faults.partition
  "The partition fault: iptables drops all traffic between groups of nodes,
  through jepsen.nemesis.combined's partition package. A target spec
  (--partition-targets) is one of:
    :one   one node, isolated from the others (the default);
    :half  a random half of the nodes, isolated from the other half (for an odd
           count, the smaller half).
  A partition lasts until the next op of this fault, a gap of --nemesis-interval
  seconds on average and at most 100 s, like every fault's. With
  --partition-duration MIN-MAX each partition lasts a whole number of seconds
  drawn from that range instead, minutes if need be, and the gaps between a
  healing and the next partition stay as they were.

  TCP keeps the connection open through the drop and retransmits once it heals,
  so this rarely makes channels reestablish; the disconnect fault does. A node
  waiting more than 30 s for its peer's reply force-closes the channel (fnn's
  PEER_CHANNEL_RESPONSE_TIMEOUT), so a partition longer than that usually closes
  the channels across it (open question 1 on the chaos-test plan)."
  (:require [clojure.string :as str]
            [jepsen.nemesis :as n]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber.schedule :as schedule])
  (:import (java.util ArrayList Collection Collections SplittableRandom)))

(def target-specs
  #{:one :half})

(defn shuffled
  "`coll` in an order drawn from `r` (Fisher-Yates)."
  [^SplittableRandom r coll]
  (let [a (ArrayList. ^Collection (vec coll))]
    (doseq [i (range (dec (count a)) 0 -1)]
      (Collections/swap a i (.nextInt r (int (inc i)))))
    (vec a)))

(defn grudge
  "The grudge of a partition of `nodes` by target spec `spec`, drawn from `r`."
  [r nodes spec]
  (case spec
    :one  (let [node (schedule/pick r nodes)]
            (n/complete-grudge [[node] (vec (remove #{node} nodes))]))
    :half (n/complete-grudge (map vec (n/bisect (shuffled r nodes))))))

(defn ops
  "Endless pairs of a partition and its healing. The grudge is computed here, not
  from a spec in the nemesis. Each partition's spec is drawn from `specs`
  (default [:one]); with only one spec nothing is drawn for it, so `(ops r nodes)`
  gives the ops it gave before target specs existed."
  ([r nodes]
   (ops r nodes [:one]))
  ([r nodes specs]
   (let [specs (vec specs)
         spec  (if (= 1 (count specs))
                 (constantly (first specs))
                 #(schedule/pick r specs))]
     (mapcat (fn [g]
               [{:type :info, :f :start-partition, :value g}
                {:type :info, :f :stop-partition, :value nil}])
             (repeatedly #(grudge r nodes (spec)))))))

(defn gaps
  "The gaps in ns between this fault's ops, which alternate between a partition
  and its healing. Without :partition-duration, every fault's gaps. With it
  ([min max] seconds), each partition lasts a whole number of seconds drawn from
  a stream of its own, and the gaps after each healing come from the usual gap
  stream."
  [streams opts]
  (let [usual (schedule/gaps (schedule/gaps-stream streams :partition) (:nemesis-interval opts))]
    (if-let [[lo hi] (:partition-duration opts)]
      (let [^SplittableRandom r (schedule/stream streams :partition-duration)]
        (interleave (repeatedly #(* 1000000000 (+ lo (.nextInt r (int (inc (- hi lo)))))))
                    usual))
      usual)))

(defn parse-duration
  "\"120-300\" -> [120 300]; \"180\" -> [180 180]. Seconds."
  [s]
  (let [[lo hi] (map parse-long (str/split s #"-" 2))]
    [lo (or hi lo)]))

(def fault
  {:faults  #{:partition}
   :stream  :partition
   :package nc/partition-package
   :ops     (fn [r opts] (ops r (vec (:nodes opts)) (:partition-targets opts [:one])))
   :gaps    gaps
   :fired   {:partition #{:start-partition}}})
