(ns jepsen.fiber.closes
  "With --force-close-ok, a channel that ended closed doesn't fail the run.

  A node force-closes a channel when its peer hasn't answered for 30 s (fnn's
  PEER_CHANNEL_RESPONSE_TIMEOUT), so a pause or partition longer than that
  closes channels that had an update in flight. Whether that is expected or a
  failure is open question 1 on the chaos-test plan; until it is answered, the
  default stays: jepsen.fiber.checker counts a closed channel as a problem.

  This wraps that checker. A setup channel counts as closed when every end of it
  in the final read is Closed; then
    - its ends' problems (not ready, balances) are dropped: settlement is on
      chain, which the checker doesn't read;
    - an unfinished payment is excused when a closed channel still holds a TLC
      for its hash, since the TLC can only resolve on chain, and so are the
      pending TLCs of other channels for such hashes (a forwarder can't settle
      or fail its upstream TLC before the downstream one resolves);
    - a probe over it is excused, and a route probe (--probe-routes) is excused
      when no path of channels that are ready at both ends joins its nodes.
  Everything else fails as before: a channel only one end considers closed, a
  stuck payment no closed channel explains, a flipped success, a probe that
  should have got through. The result lists what was closed and what it
  excused under :force-closed.

  With --force-close-ok-after SECONDS instead, a closed channel is excused only
  when a fault could have run out fnn's peer deadline on it: one of its ends
  was killed or paused, or a partition cut its ends apart, for longer than
  SECONDS (`outages`). That is open question 1 answered \"expected\", and no
  more: a channel closed with no such fault behind it (a clock step, a
  panicked actor, a slow node) still fails the run, and :not-excused lists it."
  (:require [jepsen.checker :as checker]
            [jepsen.fiber.checker :as fc]
            [jepsen.fiber.faults :as faults]))

(defn final-read
  [history]
  (->> history
       (filter #(and (= :ok (:type %)) (= :read (:f %))))
       last
       :value))

(defn ends
  "{channel-id {node channel}} of the setup channels in the final read."
  [initial channels]
  (let [ids (set (for [[_ chs] initial, ch chs] (:channel_id ch)))]
    (select-keys (fc/by-channel channels) ids)))

(defn closed-ids
  "Ids of the channels every end of which is Closed."
  [ends]
  (set (for [[id views] ends
             :when (and (seq views) (every? #(= "closed" (fc/state-name %)) (vals views)))]
         id)))

(defn ready-graph
  "{node #{node ...}}: neighbours over channels ready at both ends."
  [ends]
  (reduce (fn [m [_ views]]
            (let [[a b] (keys views)]
              (if (and b (every? #(= "channelready" (fc/state-name %)) (vals views)))
                (-> m (update a (fnil conj #{}) b) (update b (fnil conj #{}) a))
                m)))
          {}
          ends))

(defn reachable?
  [graph from to]
  (loop [seen #{from}, todo [from]]
    (if-let [[x & more] (seq todo)]
      (if (= x to)
        true
        (let [new (remove seen (graph x))]
          (recur (into seen new) (into (vec more) new))))
      false)))

(defn channel-between
  "The id of the setup channel between nodes a and b, or nil."
  [ends a b]
  (some (fn [[id views]] (when (and (contains? views a) (contains? views b)) id)) ends))

(defn outages
  "The spells in `history` during which a fault kept a node from answering its
  peers, or two nodes from reaching each other, from the faults' completions:
    {:kind :kill,      :node n}      from a :kill naming n until a :start naming it;
    {:kind :pause,     :node n}      from a :pause that stopped n until a :resume;
    {:kind :partition, :nodes #{a b}} from a :start-partition that cut a off b
                                      until a :stop-partition;
  each with :from and :to, the ops' times in ns, and :seconds. A spell nothing
  ended lasts until the last op of the history."
  [history]
  (let [end   (or (:time (last history)) 0)
        close (fn [spell to]
                (assoc spell :to to :seconds (/ (Math/round (/ (- to (:from spell)) 1e8)) 10.0)))
        shut  (fn [open out pred t]
                (let [done (filter (comp pred key) open)]
                  [(apply dissoc open (map key done))
                   (into out (map #(close (val %) t)) done)]))
        start (fn [open k spell] (if (contains? open k) open (assoc open k spell)))]
    (loop [[op & more] (faults/fault-completions history), open {}, out []]
      (if (nil? op)
        (into out (map #(close (val %) end)) (sort-by (comp :from val) open))
        (let [t (:time op)
              v (:value op)]
          (case (:f op)
            :kill  (recur more
                          (reduce (fn [o n] (start o [:kill n] {:kind :kill, :node n, :from t}))
                                  open (when (map? v) (keys v)))
                          out)
            :start (let [ns    (set (when (map? v) (keys v)))
                         [o d] (shut open out (fn [[k n]] (and (= :kill k) (ns n))) t)]
                     (recur more o d))
            :pause (recur more
                          (reduce (fn [o [n st]]
                                    (if (and (string? st) (.startsWith ^String st "T"))
                                      (start o [:pause n] {:kind :pause, :node n, :from t})
                                      o))
                                  open (when (map? v) v))
                          out)
            :resume (let [[o d] (shut open out (fn [[k]] (= :pause k)) t)]
                      (recur more o d))
            :start-partition
            (let [grudge (when (and (vector? v) (= :isolated (first v))) (second v))
                  pairs  (set (for [[a bs] grudge, b bs :when (not= a b)] #{a b}))]
              (recur more
                     (reduce (fn [o p] (start o [:partition p] {:kind :partition, :nodes p, :from t}))
                             open pairs)
                     out))
            :stop-partition (let [[o d] (shut open out (fn [[k]] (= :partition k)) t)]
                              (recur more o d))
            (recur more open out)))))))

(defn explainer
  "(fn [ends]) -> the longest spell of `outages` over `seconds` that touched
  the channel between the two nodes `ends`, or nil."
  [history seconds]
  (let [long (filter #(< seconds (:seconds %)) (outages history))]
    (fn [[a b]]
      (->> long
           (filter (fn [{:keys [node nodes]}]
                     (if nodes (= nodes #{a b}) (#{a b} node))))
           (sort-by (comp - :seconds))
           first))))

(defn- spell-summary
  [{:keys [kind node nodes seconds]}]
  (cond-> {:kind kind, :seconds seconds}
    node  (assoc :node node)
    nodes (assoc :nodes (vec (sort nodes)))))

(defn excuse
  "The fiber checker's result `r`, less what the closed channels explain. With
  `explain` ((fn [ends]) -> a reason or nil, see `explainer`), only the closed
  channels it gives a reason for are excused; the others stay problems and are
  listed under :not-excused."
  ([r initial read]
   (excuse r initial read nil))
  ([r initial read explain]
   (let [ends      (ends initial (:channels read))
         all       (closed-ids ends)
         why       (into {} (for [id all]
                              [id (if explain (explain (vec (sort (keys (get ends id))))) true)]))
         closed    (set (filter why all))
         unexcused (vec (sort (remove closed all)))
         reasons   (when explain
                     (into (sorted-map) (for [id closed] [id (spell-summary (why id))])))]
     (if (empty? closed)
       (assoc r :force-closed (cond-> {:channels []}
                                (seq unexcused) (assoc :not-excused unexcused)))
       (let [held     (set (for [id closed, ch (vals (get ends id)), tlc (:pending_tlcs ch)]
                             (:payment_hash tlc)))
             graph    (ready-graph ends)
             probe-ok (fn [{:keys [from to route?]}]
                        (if route?
                          (not (reachable? graph from to))
                          (contains? closed (channel-between ends from to))))
             split    (fn [pred xs] [(vec (remove pred xs)) (vec (filter pred xs))])
             ; Pending TLCs of an open channel, all for payments a closed one holds.
             held-tlcs? (fn [{:keys [channel node]}]
                          (every? (comp held :payment_hash)
                                  (:pending_tlcs (get-in ends [channel node]))))
             problems (for [p (:channel-problems r)]
                        (if (and (not (closed (:channel p))) (held-tlcs? p))
                          (update p :problems
                                  (partial filterv #(not= :pending-tlcs (first %))))
                          p))
             [problems p-excused] (split #(or (closed (:channel %)) (empty? (:problems %)))
                                         problems)
             [unfinished u-excused] (split #(held (:hash %)) (:unfinished-payments r))
             [probes pr-excused] (split probe-ok (:probe-failures r))]
         (assoc r
                :valid?              (and (empty? problems) (empty? unfinished)
                                          (empty? (:success-flipped r)) (empty? probes))
                :channel-problems    problems
                :unfinished-payments unfinished
                :probe-failures      probes
                :force-closed        (cond-> {:channels (vec (sort closed))
                                              :states   (frequencies (for [id closed, ch (vals (get ends id))]
                                                                       (:state ch)))
                                              :excused  {:channel-problems    p-excused
                                                         :unfinished-payments u-excused
                                                         :probe-failures      pr-excused}}
                                       reasons          (assoc :because reasons)
                                       (seq unexcused)  (assoc :not-excused unexcused))))))))

(defn checker
  "jepsen.fiber.checker/checker, with closed channels excused as above: every
  one, or with `after-s` (--force-close-ok-after) only those a kill, pause or
  partition of more than `after-s` seconds explains."
  ([]
   (checker nil))
  ([after-s]
   (let [inner (fc/checker)]
     (reify checker/Checker
       (check [_ test history opts]
         (let [r    (checker/check inner test history opts)
               read (final-read history)]
           (cond-> (if read
                     (excuse r @(:initial-channels test) read
                             (when after-s (explainer history after-s)))
                     r)
             after-s (assoc :force-close-ok-after-s after-s))))))))
