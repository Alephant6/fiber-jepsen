(ns jepsen.fiber.closes
  "When a channel that ended closed doesn't fail the run.

  A node force-closes a channel when its peer hasn't answered an update for 30 s
  (fnn's PEER_CHANNEL_RESPONSE_TIMEOUT). The Fiber team's answer to the plan's
  first open question is that this is expected once a pause or a partition has
  outlasted the deadline. So by default a closed channel is a problem
  (jepsen.fiber.checker), and the scenarios whose faults last longer than 30 s
  run with --force-close-ok-after 30.

  --force-close-ok-after SECONDS excuses a setup channel that ended closed
  (every end of it Closed in the final read) when both of these hold:

    1. An outage touched it for longer than SECONDS (`outages`): one of its
       ends was killed, paused or powered off, or a partition cut its ends
       apart. --force-close-ok-kinds says which of these count.
    2. fnn's logs agree, where they can tell (`log-closes`). The first end to
       force-close the channel did so while such an outage had lasted SECONDS
       (less a margin of 5 s), and at most SECONDS plus fnn's channel check
       (60 s) after it ended; and it logged no other reason than its peer's
       silence. For the peer deadline fnn logs `is inactive for a time`, or
       nothing at all (the V2 deadline, checked in channel maintenance, is
       silent). A close it logged another reason for is not the peer deadline
       at work: the revocation nonce ledger unbalanced, a reestablish that
       timed out, a commitment_signed that failed to verify.

  The logs can't tell when no end logged a forced close of the channel, when
  the history has no wall-clock time to place fnn's timestamps (:wall-ms on the
  nemesis's completions), or when a fault moved fnn's clock (clock,
  --outage-jump). Then 1 alone decides, and the result says so
  (:logs-not-used).

  A closed channel that isn't excused fails the run as before: one closed
  without an outage behind it (a clock step, a panicked actor, a slow node),
  before the outage had lasted the deadline, long after it ended, or for
  another reason. :not-excused lists them, and :not-excused-why says which.

  The rule judges a channel only once every end of it is Closed, and an end
  can take minutes to get there. The end that force-closes is Closed as soon
  as its commitment transaction confirms, but fnn notices a counterparty's
  forced close only in a check every 300 s (jepsen.fiber.channels): until
  then the other end stays ChannelReady (a node that was down while its peer
  closed), or ShuttingDown when it force-closed too and its own transaction
  lost. So with --force-close-ok-after the run waits for the ends to agree
  before the recovery window, the probe and the final read: the
  :settle-channels operation, for up to --channel-settle-timeout seconds, and
  only when a channel needs it. The result has the wait under :settle-wait. A
  channel one end of which is Closed and another still isn't after that wait
  is not judged: it stays a problem, and :not-settled lists it.

  What an excused channel excuses:
    - its ends' problems (not ready, balances): settlement is on chain, which
      the checker doesn't read;
    - an unfinished payment, when a closed channel still holds a TLC for its
      hash, since the TLC can only resolve on chain, and the pending TLCs of
      other channels for such hashes (a forwarder can't settle or fail its
      upstream TLC before the downstream one resolves);
    - a probe over it, and a route probe (--probe-routes) when no path of
      channels that are ready at both ends joins its nodes.
    - in the availability check (--availability-window), the payments between
      nodes that the excused closes left without a path of setup channels: see
      `availability-checker`.
  Everything else fails as before: a channel only one end considers closed
  (:not-settled), a stuck payment no closed channel explains, a flipped
  success, a probe that should have got through. The result lists what was
  closed and what it excused under :force-closed.

  --force-close-ok excuses every closed channel, whatever closed it: for the
  scenarios whose point is a close (C-03's expired TLCs, C-11's old state)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [store :as store]]
            [jepsen.fiber [checker :as fc]
                          [health :as health]])
  (:import (java.time Instant)))

(def kinds
  "The kinds of outage `outages` finds: what --force-close-ok-kinds may name."
  #{:kill :pause :partition :power :outage})

(def margin-s
  "How many seconds before an outage has lasted the deadline a close may come
  and still be the deadline's: a peer was already waiting for an answer when
  the fault began (closes came 28 to 30 s after a kill)."
  5)

(def check-interval-s
  "fnn checks its channels' deadlines once a minute (CHECK_CHANNELS_INTERVAL in
  a release build), so a node that comes back after its deadline has passed
  may take that long to close."
  60)

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

(defn unsettled-ids
  "Ids of the channels some end of which is Closed while another isn't: it
  hasn't noticed the close yet, or never will."
  [ends]
  (set (for [[id views] ends
             :let  [closed? #(= "closed" (fc/state-name %))]
             :when (and (some closed? (vals views)) (not-every? closed? (vals views)))]
         id)))

(defn settle-wait
  "What the :settle-channels wait before the final read found
  (jepsen.fiber.channels/settle!): {:waited-s, :settled?}, with :at-end, the
  ends still unsettled, when it gave up. nil when the run had no such wait, or
  every channel was settled when it began."
  [history]
  (let [v (->> history
               (filter #(and (= :ok (:type %)) (= :settle-channels (:f %))))
               last
               :value)]
    (when (and (map? v) (or (:at-start v) (false? (:settled? v))))
      (select-keys v [:waited-s :settled? :at-end]))))

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

;; Outages

(defn outages
  "The spells in `history` during which a fault kept a node from answering its
  peers, or two nodes from reaching each other, from the faults' ops:
    {:kind :kill,      :node n}       from a :kill naming n until a :start naming it;
    {:kind :pause,     :node n}       from a :pause that stopped n until a :resume;
    {:kind :power,     :node n}       from a :power-off of n until the :power-on
                                      that started it;
    {:kind :outage,    :node n}       an :outage op's time with n down;
    {:kind :partition, :nodes #{a b}} from a :start-partition that cut a off b,
                                      either way, until a :stop-partition;
  each with :from and :to, the times in ns at which the fault's op and the
  healing op completed, :seconds between them, and :began, when the fault's op
  was invoked (a SIGTERM takes fnn a while). A spell nothing ended lasts until
  the last op of the history."
  [history]
  (let [end   (or (:time (last history)) 0)
        close (fn [spell to]
                (assoc spell :to to :seconds (/ (Math/round (/ (- to (:from spell)) 1e8)) 10.0)))
        shut  (fn [open out pred t]
                (let [done (filter (comp pred key) open)]
                  [(apply dissoc open (map key done))
                   (into out (map #(close (val %) t)) done)]))
        start (fn [open k spell] (if (contains? open k) open (assoc open k spell)))
        ; The one nemesis thread runs its ops one at a time: invocation, completion.
        ops   (->> history
                   (filter #(= :nemesis (:process %)))
                   (partition 2)
                   (remove #(= :health (:f (second %)))))]
    (loop [[[inv op] & more] ops, open {}, out []]
      (if (nil? op)
        (into out (map #(close (val %) end)) (sort-by (comp :from val) open))
        (let [t     (:time op)
              v     (:value op)
              begin (fn [o kind n] (start o [kind n] {:kind kind, :node n, :began (:time inv), :from t}))]
          (case (:f op)
            :kill  (recur more (reduce #(begin %1 :kill %2) open (when (map? v) (keys v))) out)
            :start (let [ns    (set (when (map? v) (keys v)))
                         [o d] (shut open out (fn [[k n]] (and (= :kill k) (ns n))) t)]
                     (recur more o d))
            :pause (recur more
                          (reduce (fn [o [n st]]
                                    (if (and (string? st) (.startsWith ^String st "T"))
                                      (begin o :pause n)
                                      o))
                                  open (when (map? v) v))
                          out)
            :resume (let [[o d] (shut open out (fn [[k]] (= :pause k)) t)]
                      (recur more o d))
            :power-off (recur more (reduce #(begin %1 :power %2) open (when (map? v) (keys v))) out)
            :power-on  (let [ns    (set (when (map? v) (for [[n r] v :when (= :started r)] n)))
                             [o d] (shut open out (fn [[k n]] (and (= :power k) (ns n))) t)]
                         (recur more o d))
            :outage (let [{:keys [node down-ms]} (when (map? v) v)
                          from (when down-ms (- t (* 1000000 (long down-ms))))]
                      (recur more open
                             (cond-> out
                               (and node from)
                               (conj (close {:kind :outage, :node node, :began from, :from from} t)))))
            :start-partition
            (let [grudge (when (and (vector? v) (= :isolated (first v))) (second v))
                  pairs  (set (for [[a bs] grudge, b bs :when (not= a b)] #{a b}))]
              (recur more
                     (reduce (fn [o p] (start o [:partition p] {:kind  :partition, :nodes p
                                                                :began (:time inv), :from t}))
                             open pairs)
                     out))
            :stop-partition (let [[o d] (shut open out (fn [[k]] (= :partition k)) t)]
                              (recur more o d))
            (recur more open out)))))))

(defn candidates
  "(fn [ends]) -> the spells of `outages` longer than `seconds`, of a kind in
  `kinds` (default: all of them), that touched the channel between the two
  nodes `ends`; the longest first."
  ([history seconds]
   (candidates history seconds kinds))
  ([history seconds kinds]
   (let [long (->> (outages history)
                   (filter #(and (kinds (:kind %)) (< seconds (:seconds %))))
                   (sort-by (comp - :seconds))
                   vec)]
     (fn [[a b]]
       (filter (fn [{:keys [node nodes]}]
                 (if nodes (= nodes #{a b}) (#{a b} node)))
               long)))))

(defn explainer
  "(fn [ends]) -> the longest spell of `outages` over `seconds` that touched
  the channel between the two nodes `ends`, or nil."
  [history seconds]
  (comp first (candidates history seconds)))

;; fnn's logs

(def ^:private ansi
  #"\u001b\[[0-9;]*m")

(def ^:private timestamp
  #"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z")

(def close-lines
  "[reason pattern]: the lines fnn logs when it force-closes a channel, each
  pattern's group the channel's id (fnn prints it as Hash256(0x..)). The
  reasons are fnn's own (channel.rs): the peer deadline, a reestablish that
  took longer than REESTABLISH_TIMEOUT (two wordings), the revocation nonce
  ledger's watchdog, and a commitment_signed it couldn't verify. :broadcast is
  the network actor's line for every forced close, whatever the reason, and
  the only line when the reason is silent (the V2 deadline in channel
  maintenance, an expiring TLC, a shutdown RPC)."
  (let [id "(?:Hash256\\()?(0x[0-9a-f]+)\\)?"
        re #(re-pattern (str/replace % "ID" id))]
    [[:peer-deadline
      (re "Channel ID from peer .* is inactive for a time, shutting down it forcefully")]
     [:reestablish-timeout
      (re "Channel ID from peer .* has been reestablishing too long, shutting down it forcefully")]
     [:reestablish-timeout
      (re "Channel ID from peer .* waiting for channel recovery, shutting down it forcefully")]
     [:nonce-ledger
      (re "Channel ID from peer .* revocation nonce ledger has been unbalanced .* shutting down it forcefully")]
     [:commitment-signed
      (re "Failed to verify commitment_signed message.* shutdown channel ID forcefully")]
     [:broadcast
      (re "Channel \\(ID\\) to peer .* is closed .* Broadcasting closing transaction \\(\"forcefully\"\\) now")]]))

(def excusable-reasons
  "The logged reasons the rule excuses: the peer deadline, and none (:silent)."
  #{:peer-deadline :silent})

(defn log-closes
  "The forced closes in fnn's log, a seq of lines: {channel-id {:at-ms t,
  :reason r}} with the time of the first line about each channel (ms since the
  epoch, by fnn's clock) and the reason fnn gave within 5 s of it (see
  `close-lines`), :silent when it gave none."
  [lines]
  (let [events (for [line  lines
                     :when (str/includes? line "forcefully")
                     :let  [line        (str/replace line ansi "")
                            ts          (re-find timestamp line)
                            [reason id] (some (fn [[r p]]
                                                (when-let [m (re-find p line)] [r (second m)]))
                                              close-lines)]
                     :when (and ts id)]
                 {:channel id, :reason reason, :at-ms (.toEpochMilli (Instant/parse ts))})]
    (into {}
          (for [[id es] (group-by :channel events)]
            (let [es (sort-by :at-ms es)
                  t  (:at-ms (first es))]
              [id {:at-ms  t
                   :reason (or (some #(when (and (not= :broadcast (:reason %))
                                                 (<= (- (:at-ms %) t) 5000))
                                        (:reason %))
                                     es)
                               :silent)}])))))

(defn read-closes
  "`log-closes` of `node`'s fnn.log in the test's store; nil without the file."
  [test node]
  (let [f (store/path test (name node) health/log-name)]
    (when (.exists (io/file f))
      (with-open [r (io/reader f)]
        (log-closes (line-seq r))))))

(defn wall-origin-ms
  "The wall-clock time, in ms since the epoch, of the history's time 0, from
  the first op that carries :wall-ms (the nemesis's completions, see
  jepsen.fiber.health/compose); nil without one."
  [history]
  (when-let [op (first (filter #(and (:wall-ms %) (:time %)) history))]
    (- (:wall-ms op) (quot (:time op) 1000000))))

(defn clocks-moved?
  "Whether the test moves fnn's clock, so that its log's timestamps can't be
  placed in the history."
  [test]
  (boolean (or (some #{:clock} (:faults test))
               (pos? (:outage-jump test 0)))))

(defn first-close
  "The first forced close of channel `id` that any of `nodes` logged:
  {:node n, :at-ns t, :reason r}, with t in the history's time. `closes` is
  {node log-closes-result}."
  [closes origin-ms id nodes]
  (->> (for [n     nodes
             :let  [c (get-in closes [n id])]
             :when c]
         {:node n, :at-ns (* 1000000 (- (:at-ms c) origin-ms)), :reason (:reason c)})
       (sort-by :at-ns)
       first))

(defn- spell-summary
  [{:keys [kind node nodes seconds]}]
  (cond-> {:kind kind, :seconds seconds}
    node  (assoc :node node)
    nodes (assoc :nodes (vec (sort nodes)))))

(defn- tenths
  [ns]
  (/ (Math/round (/ ns 1e8)) 10.0))

(defn judge
  "Whether the rule excuses the closed channel `id` between the nodes `ends`:
  {:excused? true, :because spell} or {:excused? false, :why w}, with what the
  logs said of the close (:closed-by, :closed-at-s in the history's time,
  :logged) when they said anything. `spells` is (fn [ends]) -> the outages that
  count (`candidates`); `closes` and `origin-ms` are nil when the logs aren't
  used. :why is :no-outage, :another-reason (fnn logged a reason the rule
  doesn't cover), or :outside-the-outages (the close came before any outage
  had lasted `seconds`, or too long after it ended)."
  [{:keys [seconds spells closes origin-ms]} id ends]
  (let [cands  (spells ends)
        close  (when (and closes origin-ms) (first-close closes origin-ms id ends))
        logged (when close
                 {:closed-by   (:node close)
                  :closed-at-s (tenths (:at-ns close))
                  :logged      (:reason close)})
        in?    (fn [{:keys [began to]}]
                 (<= (+ began (* 1000000000 (- seconds margin-s)))
                     (:at-ns close)
                     (+ to (* 1000000000 (+ seconds check-interval-s)))))]
    (merge
      logged
      (cond
        (empty? cands)
        {:excused? false, :why :no-outage}

        (nil? close)
        {:excused? true, :because (spell-summary (first cands))}

        (not (excusable-reasons (:reason close)))
        {:excused? false, :why :another-reason}

        :else
        (if-let [s (first (filter in? cands))]
          {:excused? true, :because (spell-summary s)}
          {:excused? false
           :why      :outside-the-outages
           :outages  (mapv #(assoc (spell-summary %) :from-s (tenths (:began %)) :to-s (tenths (:to %)))
                           cands)})))))

(defn rule
  "The --force-close-ok-after rule for a run: {:explain (fn [id ends]) -> a
  judgement (see `judge`)}, and :logs-not-used with the reason when fnn's logs
  can't place the closes. Options:
    :seconds      the deadline;
    :kinds        the kinds of outage that count (default: `kinds`);
    :read-closes  (fn [test node]) -> `log-closes` of the node's log, or nil
                  (default: `read-closes`)."
  [test history {:keys [seconds], :as opts}]
  (let [spells  (candidates history seconds (or (:kinds opts) kinds))
        origin  (wall-origin-ms history)
        skip    (cond (clocks-moved? test) :clocks-moved
                      (nil? origin)        :no-wall-clock)
        read    (or (:read-closes opts) read-closes)
        ; Read only once a channel turns out to be closed.
        closes  (delay (into {} (for [node (:nodes test)]
                                  [node (try (read test node)
                                             (catch Exception e
                                               (warn e "Couldn't read" health/log-name "of" node)
                                               nil))])))]
    (cond-> {:explain (fn [id ends]
                        (judge {:seconds   seconds
                                :spells    spells
                                :closes    (when-not skip @closes)
                                :origin-ms (when-not skip origin)}
                               id ends))}
      skip (assoc :logs-not-used skip))))

;; The checker

(defn- tally
  "\"3: kill 2, partition 1\" for a seq of keywords; nil for none."
  [ks]
  (when (seq ks)
    (str (count ks) ": "
         (str/join ", " (for [[k n] (sort-by (comp name key) (frequencies ks))]
                          (str (name k) " " n))))))

(defn excuse
  "The fiber checker's result `r`, less what the closed channels explain. With
  `explain` ((fn [id ends]) -> a judgement, see `judge`), only the closed
  channels it excuses are; the others stay problems and are listed under
  :not-excused, with the judgements under :not-excused-why. A channel only
  some end of which is Closed is not judged either way: it stays a problem,
  listed under :not-settled."
  ([r initial read]
   (excuse r initial read nil))
  ([r initial read explain]
   (let [ends      (ends initial (:channels read))
         all       (closed-ids ends)
         unsettled (vec (sort (unsettled-ids ends)))
         why       (into {} (for [id all]
                              [id (if explain
                                    (explain id (vec (sort (keys (get ends id)))))
                                    {:excused? true})]))
         closed    (set (filter (comp :excused? why) all))
         unexcused (vec (sort (remove closed all)))
         detail    (fn [ids]
                     (into (sorted-map)
                           (for [id ids]
                             [id (let [j (dissoc (why id) :excused?)]
                                   (merge (:because j) (dissoc j :because)))])))
         reasons   (when explain (detail closed))
         why-not   (when (and explain (seq unexcused)) (detail unexcused))
         summary   (cond-> {}
                     (seq closed)    (assoc :closes-excused
                                            (if explain
                                              (tally (map (comp :kind :because why) closed))
                                              (str (count closed))))
                     (seq unexcused) (assoc :closes-not-excused (tally (map (comp :why why) unexcused)))
                     (seq unsettled) (assoc :closes-not-settled (str (count unsettled))))]
     (if (empty? closed)
       (merge r summary
              {:force-closed (cond-> {:channels []}
                               (seq unexcused) (assoc :not-excused unexcused)
                               why-not         (assoc :not-excused-why why-not)
                               (seq unsettled) (assoc :not-settled unsettled))})
       (let [held     (set (for [id closed, ch (vals (get ends id)), tlc (fc/live-tlcs ch)]
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
                                  (fc/live-tlcs (get-in ends [channel node]))))
             problems (for [p (:channel-problems r)]
                        (if (and (not (closed (:channel p))) (held-tlcs? p))
                          (update p :problems
                                  (partial filterv #(not= :pending-tlcs (first %))))
                          p))
             [problems p-excused] (split #(or (closed (:channel %)) (empty? (:problems %)))
                                         problems)
             [unfinished u-excused] (split #(held (:hash %)) (:unfinished-payments r))
             [probes pr-excused] (split probe-ok (:probe-failures r))]
         (merge
           r
           summary
           {:valid?              (and (empty? problems) (empty? unfinished)
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
                                   (seq unexcused)  (assoc :not-excused unexcused)
                                   why-not          (assoc :not-excused-why why-not)
                                   (seq unsettled)  (assoc :not-settled unsettled))}))))))

(defn checker
  "jepsen.fiber.checker/checker, with closed channels excused as above: every
  one (--force-close-ok), or with a deadline in seconds
  (--force-close-ok-after) only those the rule excuses. `opts` is the deadline
  or `rule`'s options."
  ([]
   (checker nil))
  ([opts]
   (let [inner (fc/checker)
         opts  (if (number? opts) {:seconds opts} opts)
         after (:seconds opts)]
     (reify checker/Checker
       (check [_ test history copts]
         (let [r    (checker/check inner test history copts)
               read (final-read history)
               rule (when (and after read) (rule test history opts))
               wait (settle-wait history)]
           (cond-> (if read
                     (excuse r @(:initial-channels test) read (:explain rule))
                     r)
             wait  (assoc :settle-wait wait)
             after (assoc :force-close-ok-after-s after)
             (and after (:kinds opts)) (assoc :force-close-ok-kinds (vec (sort (:kinds opts))))
             (:logs-not-used rule) (assoc-in [:force-closed :logs-not-used] (:logs-not-used rule)))))))))

;; Availability after an excused close

(defn excused-ids
  "Ids of the closed channels `excuse` excuses: every channel all of whose ends
  are Closed, or with `explain` only those it gives a reason for."
  [ends explain]
  (set (for [id (closed-ids ends)
             :when (or (nil? explain)
                       (:excused? (explain id (vec (sort (keys (get ends id)))))))]
         id)))

(defn setup-graph
  "{node #{node ...}}: neighbours over the setup channels (`initial`, the
  channels each node listed after setup), without the channels in `cut`. A
  channel that isn't ready stays in: only `cut` takes one out."
  [initial cut]
  (let [nodes (reduce (fn [m [node chs]]
                        (reduce #(update %1 (:channel_id %2) (fnil conj #{}) node) m chs))
                      {}
                      initial)]
    (reduce (fn [g [id ns]]
              (let [[a b] (sort ns)]
                (if (and b (not (contains? cut id)))
                  (-> g (update a (fnil conj #{}) b) (update b (fnil conj #{}) a))
                  g)))
            {}
            nodes)))

(defn availability
  "The availability verdict for a run in which the channels `cut` (ids) were
  closed and excused. `baseline` and `recovery` are the windows' payments
  (jepsen.fiber.checker/window-pays).

  An excused close takes a channel away for good. Two nodes that it leaves
  without a path of setup channels can't pay each other in the recovery
  window, and that is the close, not a network that failed to recover. So
  their payments are left out, of the baseline too: the same pairs are
  compared before and after. :cut-off has those pairs ([from to]) with the
  payments each window lost. When that leaves the recovery window with no
  payment at all, there is nothing to compare, and the check passes and says
  so (:nothing-to-compare). A pair with a path left is judged as ever, and so
  is a pair whose path needs a channel that is closed but not excused."
  [tolerance initial cut baseline recovery]
  (let [graph   (setup-graph initial cut)
        linked? (memoize (fn [from to] (reachable? graph from to)))
        ends-of (fn [[_ c]] [(get-in c [:value :from]) (get-in c [:value :to])])
        keep?   (fn [pair]
                  (let [[from to] (ends-of pair)]
                    (or (nil? from) (nil? to) (linked? from to))))
        lost    (fn [pairs] (frequencies (map ends-of (remove keep? pairs))))
        b-lost  (lost baseline)
        r-lost  (lost recovery)
        kept    (filterv keep? recovery)
        r       (fc/availability tolerance
                                 (fc/window-stats (filterv keep? baseline))
                                 (fc/window-stats kept))]
    (cond-> r
      (or (seq b-lost) (seq r-lost))
      (assoc :cut-off (into (sorted-map)
                            (for [pair (set (concat (keys b-lost) (keys r-lost)))]
                              [pair {:baseline (get b-lost pair 0)
                                     :recovery (get r-lost pair 0)}])))

      (and (seq recovery) (empty? kept))
      (assoc :valid? true, :nothing-to-compare true))))

(defn availability-checker
  "jepsen.fiber.checker/availability-checker for a run that excuses closed
  channels: with every closed channel excused (--force-close-ok), or with a
  deadline or `rule`'s options (--force-close-ok-after) only those the rule
  excuses. See `availability`. A run without an excused close gets the plain
  check."
  ([tolerance]
   (availability-checker tolerance nil))
  ([tolerance opts]
   (let [opts  (if (number? opts) {:seconds opts} opts)
         after (:seconds opts)]
     (reify checker/Checker
       (check [_ test history _copts]
         (let [baseline (fc/window-pays history :baseline)
               recovery (fc/window-pays history :recovery)
               read     (final-read history)
               initial  (some-> (:initial-channels test) deref)
               rule     (when (and after read) (rule test history opts))
               cut      (when (and read initial)
                          (excused-ids (ends initial (:channels read)) (:explain rule)))]
           (if (seq cut)
             (assoc (availability tolerance initial cut baseline recovery)
                    :excused-closes (count cut))
             (fc/availability tolerance
                              (fc/window-stats baseline)
                              (fc/window-stats recovery)))))))))
