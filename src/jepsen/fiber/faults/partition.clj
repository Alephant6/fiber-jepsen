(ns jepsen.fiber.faults.partition
  "The partition fault: iptables drops traffic between nodes, through
  jepsen.nemesis.combined's partition package. A target spec
  (--partition-targets) is one of:
    :one           one node, isolated from the others (the default);
    :half          a random half of the nodes, isolated from the other half
                   (for an odd count, the smaller half);
    :deaf          one node drops everything the others send it, and they still
                   get what it sends: a one-way partition;
    :mute          the others drop everything one node sends, and it still gets
                   what they send: the other way;
    :pair          the two ends of one channel, cut off from each other only.
                   Both still reach every other node, so the partition isn't
                   transitive: the network knows the channel, and can't use it;
    :pair-one-way  one end of a channel drops what the other sends it.

  A one-way cut lets messages through in one direction only. TCP delivers the
  open side's data once and loses every acknowledgement of it, so a node gets
  its peer's commitment_signed, say, and its own revoke_and_ack never arrives:
  each end has a different view of who is waiting for whom. A partition rule is
  one iptables DROP in a node's INPUT chain per source it no longer hears, so a
  one-way cut is a grudge that isn't symmetric. After every partition the op
  records the rules found on each node (:rules), and the fault took effect
  (`effect`) when they are exactly the grudge's.
  A partition lasts until the next op of this fault, a gap of --nemesis-interval
  seconds on average and at most 100 s, like every fault's. With
  --partition-duration MIN-MAX each partition lasts a whole number of seconds
  drawn from that range instead, minutes if need be, and the gaps between a
  healing and the next partition stay as they were.

  TCP keeps the connection open through the drop and retransmits once it heals,
  so this rarely makes channels reestablish; the disconnect fault does. A node
  waiting more than 30 s for its peer's reply force-closes the channel (fnn's
  PEER_CHANNEL_RESPONSE_TIMEOUT), so a partition longer than that usually closes
  the channels across it (open question 1 on the chaos-test plan).

  Healing deletes the partitions' own rules only (`net`), so that it doesn't
  also end a chain partition that is in effect at the time."
  (:require [clojure.string :as str]
            [jepsen [control :as c]
                    [nemesis :as n]
                    [net :as jnet]]
            [jepsen.net.proto :as p]
            [jepsen.nemesis.combined :as nc]
            [jepsen.fiber [schedule :as schedule]
                          [topology :as topology]])
  (:import (java.util ArrayList Collection Collections SplittableRandom)))

(def target-specs
  #{:one :half :deaf :mute :pair :pair-one-way})

(defn shuffled
  "`coll` in an order drawn from `r` (Fisher-Yates)."
  [^SplittableRandom r coll]
  (let [a (ArrayList. ^Collection (vec coll))]
    (doseq [i (range (dec (count a)) 0 -1)]
      (Collections/swap a i (.nextInt r (int (inc i)))))
    (vec a)))

(defn grudge
  "The grudge of a partition of `nodes` by target spec `spec`, drawn from `r`:
  {node #{the nodes whose packets it drops}}. `edges` are the topology's
  channels, for the specs that cut one."
  ([r nodes spec]
   (grudge r nodes nil spec))
  ([^SplittableRandom r nodes edges spec]
   (case spec
     :one  (let [node (schedule/pick r nodes)]
             (n/complete-grudge [[node] (vec (remove #{node} nodes))]))
     :half (n/complete-grudge (map vec (n/bisect (shuffled r nodes))))
     :deaf (let [node (schedule/pick r nodes)]
             {node (set (remove #{node} nodes))})
     :mute (let [node (schedule/pick r nodes)]
             (into {} (for [other nodes :when (not= other node)] [other #{node}])))
     :pair (let [[a b] (schedule/pick r edges)]
             {a #{b}, b #{a}})
     :pair-one-way (let [[a b] (schedule/pick r edges)]
                     (if (.nextBoolean r) {a #{b}} {b #{a}})))))

(defn ops
  "Endless pairs of a partition and its healing. The grudge is computed here, not
  from a spec in the nemesis. Each partition's spec is drawn from `specs`
  (default [:one]); with only one spec nothing is drawn for it, so `(ops r nodes)`
  gives the ops it gave before target specs existed. `edges` are the topology's
  channels, which :pair and :pair-one-way cut."
  ([r nodes]
   (ops r nodes [:one]))
  ([r nodes specs]
   (ops r nodes specs nil))
  ([r nodes specs edges]
   (let [specs (vec specs)
         edges (vec edges)
         spec  (if (= 1 (count specs))
                 (constantly (first specs))
                 #(schedule/pick r specs))]
     (when (and (some #{:pair :pair-one-way} specs) (empty? edges))
       (throw (ex-info "--partition-targets pair and pair-one-way need a channel to cut"
                       {:type :no-edges, :nodes nodes})))
     (mapcat (fn [g]
               [{:type :info, :f :start-partition, :value g}
                {:type :info, :f :stop-partition, :value nil}])
             (repeatedly #(grudge r nodes edges (spec)))))))

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

(defn rules
  "{node #{source address}}: the partition rules now on each node of `nodes`."
  [test nodes]
  (into (sorted-map)
        (c/on-nodes test nodes
                    (fn [_ _] (set (partition-sources (c/su (c/exec :iptables :-w :-S :INPUT))))))))

(defn recording
  "The partition nemesis `nemesis`, with the rules found on every node after
  each partition on the op's completion, as :rules."
  [nemesis]
  (reify
    n/Reflection
    (fs [_] (n/fs nemesis))

    n/Nemesis
    (setup! [_ test] (recording (n/setup! nemesis test)))

    (invoke! [_ test op]
      (let [op' (n/invoke! nemesis test op)]
        (cond-> op'
          (= :start-partition (:f op')) (assoc :rules (rules test (:nodes test))))))

    (teardown! [_ test] (n/teardown! nemesis test))))

(defn expected-rules
  "{node #{source address}}: the rules `grudge` means, as iptables lists them."
  [grudge]
  (into {} (for [[node sources] grudge
                 :when (seq sources)]
             [node (set (map #(str (topology/node-ip %) "/32") sources))])))

(defn effect
  "Whether a completed op took effect (see jepsen.fiber.faults): a
  :start-partition did when the rules found on the nodes are the grudge's, no
  fewer and no more, so that a cut meant to be one-way isn't two-way and a
  pair's cut doesn't isolate a node. Rules for other sources than the test's
  nodes aren't the partition's."
  [op]
  (when (= :start-partition (:f op))
    (let [v      (:value op)
          grudge (when (and (vector? v) (= :isolated (first v))) (second v))
          found  (:rules op)]
      (cond
        (nil? grudge) (str "no partition: " (pr-str v))
        (nil? found)  nil
        :else
        (let [wanted (expected-rules grudge)
              ours   (set (map #(str (topology/node-ip %) "/32") (keys found)))
              found  (into {} (for [[node sources] found
                                    :let  [sources (set (filter ours sources))]
                                    :when (seq sources)]
                                [node sources]))]
          (when (not= wanted found)
            (str "iptables rules differ from the grudge: wanted " (pr-str (into (sorted-map) wanted))
                 ", found " (pr-str (into (sorted-map) found)))))))))

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
   :package (fn [opts] (update (nc/partition-package opts) :nemesis recording))
   :ops     (fn [r opts] (ops r (vec (:nodes opts)) (:partition-targets opts [:one])
                              (topology/edges opts)))
   :gaps    gaps
   :fired   {:partition #{:start-partition}}
   :effect  effect})
