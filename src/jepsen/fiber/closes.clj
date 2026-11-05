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
  excused under :force-closed."
  (:require [jepsen.checker :as checker]
            [jepsen.fiber.checker :as fc]))

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

(defn excuse
  "The fiber checker's result `r`, less what the closed channels explain."
  [r initial read]
  (let [ends     (ends initial (:channels read))
        closed   (closed-ids ends)]
    (if (empty? closed)
      (assoc r :force-closed {:channels []})
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
               :force-closed        {:channels (vec (sort closed))
                                     :states   (frequencies (for [id closed, ch (vals (get ends id))]
                                                              (:state ch)))
                                     :excused  {:channel-problems    p-excused
                                                :unfinished-payments u-excused
                                                :probe-failures      pr-excused}})))))

(defn checker
  "jepsen.fiber.checker/checker, with closed channels excused as above."
  []
  (let [inner (fc/checker)]
    (reify checker/Checker
      (check [_ test history opts]
        (let [r    (checker/check inner test history opts)
              read (final-read history)]
          (if read
            (excuse r @(:initial-channels test) read)
            r))))))
