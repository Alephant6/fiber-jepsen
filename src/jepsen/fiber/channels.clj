(ns jepsen.fiber.channels
  "The channel workload (--channel-ops): opens, cooperatively closes and
  force-closes channels on the topology's edges while payments run, so faults
  can land in the middle of opening and closing, which setup and --close reach
  only once.

  Each operation is drawn in advance from the run's seed, from the stream
  :channels (jepsen.fiber.schedule): its kind, the edge, which end acts and,
  for a close, which of the edge's ready channels. The channel itself is
  resolved when the operation runs, among the channels that are ready then, so
  the same seed can close a different channel in another run. A close never
  takes the last --channel-keep ready channels of an edge: payments and the
  post-heal probe still need a route. A channel counts as ready only when both
  ends say so, since an end can stay ChannelReady for minutes after the other
  end force-closed it (below).

  The checker (assertion 4 of the chaos-test plan, and C-09's): after the
  faults heal, every end of every channel is ChannelReady or Closed, never in
  between (opening, shutting down, stale), and both ends of a channel agree on
  which of the two it is.

  Fiber notices that its counterparty force-closed a channel only in a check
  that runs every 300 s (CHECK_CHANNELS_SHUTDOWN_INTERVAL in fiber/network.rs at
  ae6f7d3); until then its end stays ChannelReady, or ShuttingDown when it
  force-closed too and waits for its own commitment transaction, which can no
  longer confirm. So after the quiet period, and before the probe, the
  :settle-channels operation waits until every channel is settled in this sense,
  for up to --channel-settle-timeout seconds, and records how long it took."
  (:require [jepsen.checker :as checker]
            [jepsen.fiber [checker :as fc]
                          [client :as client]
                          [rpc :as rpc]
                          [schedule :as schedule]
                          [topology :as topology]])
  (:import (java.util SplittableRandom)))

(def kinds
  "--channel-ops names -> the client operation."
  {:open        :open-channel
   :close       :close-channel
   :force-close :force-close})

(def max-pick
  "Close ops pick a channel by an index drawn below this, modulo the edge's
  ready channels."
  1024)

(defn ops
  "Endless channel ops drawn from `r`, of the kinds in :channel-ops."
  [^SplittableRandom r opts]
  (let [kinds* (mapv kinds (:channel-ops opts))
        edges  (count (topology/edges opts))]
    (when (seq kinds*)
      (repeatedly
        (fn []
          (let [f    (schedule/pick r kinds*)
                edge (.nextInt r (int (max 1 edges)))
                flip (.nextBoolean r)
                pick (.nextInt r (int max-pick))]
            {:f f, :value (cond-> {:edge edge, :flip? flip}
                            (not= :open-channel f) (assoc :pick pick))}))))))

(defn generator
  "The channel ops for the test options, paced by gaps averaging
  :channel-interval seconds; nil without --channel-ops."
  [streams opts]
  (when (seq (:channel-ops opts))
    (schedule/paced (schedule/gaps (schedule/gaps-stream streams :channels)
                                   (:channel-interval opts 20))
                    (ops (schedule/stream streams :channels) opts))))

(defn- edge-ends
  "The [actor peer] of op `value` on the test's edges."
  [test {:keys [edge flip?]}]
  (let [edges (vec (topology/edges test))
        [a b] (nth edges (mod edge (count edges)))]
    (if flip? [b a] [a b])))

(defn ready-channels
  "Ids of `node`'s ChannelReady channels with `peer`, sorted."
  [test node peer]
  (let [pubkey (get @(:pubkeys test) peer)]
    (->> (:channels (rpc/call node "list_channels" {:pubkey pubkey}))
         (filter #(= "channelready" (fc/state-name %)))
         (map :channel_id)
         sort
         vec)))

(defn edge-ready-channels
  "Ids of the channels between `a` and `b` that both ends report
  ChannelReady, sorted. When `b` can't be read, `a`'s view alone, with
  :peer-view :unavailable."
  [test a b]
  (let [mine (ready-channels test a b)]
    (try {:ready (vec (sort (filter (set (ready-channels test b a)) mine)))}
         (catch Exception _
           {:ready mine, :peer-view :unavailable}))))

(defn- attempt
  "Calls the RPC; :ok-shaped op on success, :fail with the error otherwise.
  A timeout leaves the outcome unknown: :info."
  [op value f]
  (try (let [res (f)]
         (assoc op :type :ok, :value (merge value res)))
       (catch clojure.lang.ExceptionInfo e
         (if (= :rpc-error (:type (ex-data e)))
           (assoc op :type :fail, :value value, :error (ex-message e))
           (throw e)))
       (catch java.net.ConnectException _
         (assoc op :type :fail, :value value, :error :node-down))
       (catch org.apache.http.conn.ConnectTimeoutException _
         (assoc op :type :fail, :value value, :error :node-unreachable))
       (catch java.io.IOException e
         (assoc op :type :info, :value value, :error [:no-reply (.getSimpleName (class e))]))))

(defn open!
  [test op]
  (let [[a b] (edge-ends test (:value op))
        value (assoc (:value op) :by a, :peer b)]
    (attempt op value
             #(let [res (rpc/call a "open_channel"
                                  {:pubkey         (get @(:pubkeys test) b)
                                   :funding_amount (rpc/int->hex (* (:channel-funding test 500)
                                                                    topology/shannons-per-ckb))
                                   :public         true}
                                  {:timeout 10000})]
                {:temporary-channel (:temporary_channel_id res)}))))

(defn close!
  "Closes one of the edge's ready channels (edge-ready-channels), cooperatively
  or by force, from the op's acting end. When the edge has --channel-keep ready
  channels or fewer, does nothing: the op is :ok with :skipped :keeping-last,
  since keeping them is the workload's choice, not a failure (and Jepsen's stats
  checker would fail a run in which every close of a kind was a :fail)."
  [test op force?]
  (let [[a b] (edge-ends test (:value op))
        value (assoc (:value op) :by a, :peer b)]
    (try
      (let [{:keys [ready peer-view]} (edge-ready-channels test a b)
            value (cond-> value peer-view (assoc :peer-view peer-view))
            keep  (:channel-keep test 1)]
        (if (<= (count ready) keep)
          (assoc op :type :ok, :value (assoc value :ready (count ready), :skipped :keeping-last))
          (let [ch    (nth ready (mod (:pick value 0) (count ready)))
                value (assoc value :channel ch, :ready (count ready))]
            (attempt op value
                     #(do (rpc/call a "shutdown_channel"
                                    (if force?
                                      {:channel_id ch, :force true}
                                      {:channel_id ch, :fee_rate (rpc/int->hex 1000)})
                                    {:timeout 10000})
                          {})))))
      (catch java.net.ConnectException _
        (assoc op :type :fail, :value value, :error :node-down))
      (catch Exception e
        (assoc op :type :fail, :value value, :error (or (ex-message e) (str (class e))))))))

(defmethod client/invoke-op :open-channel
  [test _node op]
  (open! test op))

(defmethod client/invoke-op :close-channel
  [test _node op]
  (close! test op false))

(defmethod client/invoke-op :force-close
  [test _node op]
  (close! test op true))

(def settled-states
  #{"channelready" "closed"})

(defn unsettled
  "In {node [channel ...]}: the channel ends that are neither ready nor closed
  (:between), and the channels whose ends disagree on which of the two they are
  (:disagree)."
  [channels-by-node]
  (let [ends (fc/by-channel channels-by-node)]
    {:between  (vec (for [[id es] ends
                          [node ch] es
                          :when (not (settled-states (fc/state-name ch)))]
                      {:channel id, :node node, :state (:state ch)}))
     :disagree (vec (for [[id es] ends
                          :let  [states (set (map fc/state-name (vals es)))]
                          :when (and (= 2 (count es)) (every? settled-states states)
                                     (< 1 (count states)))]
                      {:channel id
                       :states  (into (sorted-map) (for [[n ch] es] [n (:state ch)]))}))}))

(def ^:const settle-poll-ms 5000)

(defn settle!
  "Waits until no channel is unsettled on any node, for up to
  :channel-settle-timeout seconds: {:waited-s, :settled?, and what was still
  unsettled at the first and the last poll}."
  [test]
  (let [start    (System/currentTimeMillis)
        deadline (+ start (* 1000 (:channel-settle-timeout test 360)))
        read     #(unsettled (into {} (for [n (:nodes test)] [n (client/read-node n)])))
        first-u  (read)]
    (loop [u first-u]
      (let [settled? (and (empty? (:between u)) (empty? (:disagree u)))]
        (if (or settled? (< deadline (System/currentTimeMillis)))
          (cond-> {:waited-s (quot (- (System/currentTimeMillis) start) 1000)
                   :settled? settled?}
            (not= first-u {:between [], :disagree []}) (assoc :at-start first-u)
            (not settled?)                             (assoc :at-end u))
          (do (Thread/sleep settle-poll-ms)
              (recur (read))))))))

(defmethod client/invoke-op :settle-channels
  [test _node op]
  (assoc op :type :ok, :value (settle! test)))

(defn analyze
  "Channel ends that are neither ready nor closed in the final read, and
  channels whose ends disagree on which of the two they are."
  [read history]
  (let [ends     (fc/by-channel (:channels read))
        {:keys [between disagree]} (unsettled (:channels read))
        settle   (->> history
                      (filter #(and (= :ok (:type %)) (= :settle-channels (:f %))))
                      last
                      :value)
        ops      (->> history
                      (filter #(and (#{:open-channel :close-channel :force-close} (:f %))
                                    (#{:ok :fail :info} (:type %))))
                      (map (fn [op] [(:f op) (if (get-in op [:value :skipped]) :skipped (:type op))]))
                      frequencies)]
    {:valid?             (and (empty? between) (empty? disagree))
     :ops                (into (sorted-map) (for [[[f t] n] ops] [(str (name f) " " (name t)) n]))
     :final-states       (frequencies (for [[_ es] ends, [_ ch] es]
                                        (let [s (fc/state-name ch)]
                                          (if (= "closed" s)
                                            (str "Closed " (get-in ch [:state :state_flags]))
                                            (get-in ch [:state :state_name])))))
     :between-states     between
     :ends-disagree      disagree
     :settle-wait        (select-keys settle [:waited-s :settled? :at-start])
     :closed-on-purpose  (count (fc/closed-on-purpose history))}))

(defn checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [read (->> history
                      (filter #(and (= :ok (:type %)) (= :read (:f %))))
                      last
                      :value)]
        (if read
          (analyze read history)
          {:valid? :unknown, :error "no final read"})))))
