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
  the channels across it, which the Fiber team expects
  (--force-close-ok-after, jepsen.fiber.closes).

  Healing deletes the partitions' own rules only (`net`), so that it doesn't
  also end a chain partition that is in effect at the time."
  (:require [clojure.string :as str]
            [jepsen [control :as c]
                    [nemesis :as n]
                    [net :as jnet]]
            [jepsen.net.proto :as p]
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
  stream. Either way, no gap after a healing is shorter than :nemesis-min-gap
  seconds."
  [streams opts]
  (let [usual (schedule/gaps (schedule/gaps-stream streams :partition) (:nemesis-interval opts))]
    (schedule/after-recovery-at-least
      (:nemesis-min-gap opts)
      (if-let [[lo hi] (:partition-duration opts)]
        (let [^SplittableRandom r (schedule/stream streams :partition-duration)]
          (interleave (repeatedly #(* 1000000000 (+ lo (.nextInt r (int (inc (- hi lo)))))))
                      usual))
        usual))))

(def partition-rule
  "A rule of jepsen.net/iptables's partitions as `iptables -S INPUT` prints it:
  everything from one address dropped, with no match or comment that would
  mark another fault's rule (the chain partition's carry a comment)."
  #"-A INPUT -s (\S+) -j DROP")

(defn partition-sources
  "The source address of every partition rule in `s`, the output of
  `iptables -S INPUT`, once per rule."
  [s]
  (keep #(second (re-matches partition-rule (str/trim %))) (str/split-lines (or s ""))))

(defn heal-partitions!
  "Deletes the current node's partition rules, and no other rule."
  []
  (c/su (doseq [src (partition-sources (c/exec :iptables :-w :-S :INPUT))]
          (c/exec :iptables :-w :-D :INPUT :-s src :-j :DROP))))

(def net
  "The test's network (its :net): jepsen.net/iptables, except that healing
  deletes only the partitions' rules. iptables' own heal! flushes every rule on
  every node, so a partition's healing used to end any chain partition
  (jepsen.fiber.faults.chain-partition) in effect at the time, whenever both
  faults ran (C-16)."
  (let [base jnet/iptables]
    (reify
      p/Net
      (drop! [_ test src dest] (p/drop! base test src dest))
      (heal! [_ test] (c/on-nodes test (fn [_ _] (heal-partitions!))))
      (slow! [_ test] (p/slow! base test))
      (slow! [_ test opts] (p/slow! base test opts))
      (flaky! [_ test] (p/flaky! base test))
      (fast! [_ test] (p/fast! base test))
      (shape! [_ test nodes behavior] (p/shape! base test nodes behavior))

      p/PartitionAll
      (drop-all! [_ test grudge] (p/drop-all! base test grudge)))))

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
