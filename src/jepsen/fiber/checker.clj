(ns jepsen.fiber.checker
  "Invariants checked against the final read, which happens after every fault
  has been healed and the network has had time to converge:

  - every channel opened during setup is still ChannelReady, unless a workload
    (--channel-ops) closed it on purpose;
  - no channel still holds pending TLCs, except TLCs for payments still
    pending on a channel a workload closed: the forwarder resolves the upstream
    TLC only once the downstream one resolves on chain;
  - both ends of a channel agree on its balances (A's local = B's remote);
  - each channel's total (local + remote) equals its total at the start of the
    test: payments move money within a channel, they never create or destroy it;
  - every payment reached Success or Failed, except one whose TLC is still
    pending on a channel a workload closed: that settles on chain, by preimage
    or after it expires (hours, in a release build);
  - no payment the client saw succeed later reports a different status;
  - a fresh payment each way over every channel (the probe) succeeds: the
    network can still move money once faults are gone. A direction is excused
    only if the final read shows its sender's balance on that channel below the
    probe amount on every ready channel to the receiver."
  (:require [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.fiber.rpc :as rpc]))

(defn state-name
  [channel]
  (-> channel :state :state_name str (str/replace "_" "") str/lower-case))

(defn amount
  [channel k]
  (or (rpc/hex->int (get channel k)) 0))

(defn sender-balance
  "The sender's largest local balance on a ChannelReady channel with `to` in the
  final read (a probe can use any one of them), or nil when it has none."
  [channels-by-node from to]
  (let [peer-channel? (set (map :channel_id (get channels-by-node to)))
        balances      (for [ch    (get channels-by-node from)
                            :when (and (peer-channel? (:channel_id ch))
                                       (= "channelready" (state-name ch)))]
                        (amount ch :local_balance))]
    (when (seq balances) (reduce max balances))))

(defn total
  "Fiber only moves an in-flight TLC's amount out of local_balance when the TLC
  settles (offered/received_tlc_balance just list in-flight TLCs), so the
  conserved quantity is local + remote."
  [channel]
  (+ (amount channel :local_balance)
     (amount channel :remote_balance)))

(defn by-channel
  "{node [channel ...]} -> {channel-id {node channel}}"
  [channels-by-node]
  (reduce-kv (fn [m node channels]
               (reduce (fn [m ch] (assoc-in m [(:channel_id ch) node] ch)) m channels))
             {}
             channels-by-node))

(defn closed-on-purpose
  "Ids of the channels a workload (--channel-ops) asked to close, cooperatively
  or by force, whether or not the reply arrived."
  [history]
  (set (for [op    history
             :when (and (#{:close-channel :force-close} (:f op))
                        (#{:ok :info} (:type op))
                        (get-in op [:value :channel]))]
         (get-in op [:value :channel]))))

(defn held-on-chain
  "Hashes of the TLCs still pending, on either end's final record, on the
  channels in `ids`. After a force close such a TLC settles on chain: when its
  receiver claims it with the preimage, or once it expires."
  [final ids]
  (set (for [[id views] (by-channel final)
             :when      (ids id)
             [_ ch]     views
             tlc        (:pending_tlcs ch)]
         (:payment_hash tlc))))

(defn excuse-held
  "Drops a channel's :pending-tlcs problem when every TLC pending on it is for
  a payment in `held` (pending on a channel a workload closed): a forwarder
  can't settle or fail the upstream TLC until the downstream one resolves on
  chain. Entries left without problems go."
  [problems channels-by-node held]
  (let [pending (into {} (for [[node chs] channels-by-node, ch chs]
                           [[node (:channel_id ch)] (map :payment_hash (:pending_tlcs ch))]))]
    (vec (for [p     problems
               :let  [hashes (get pending [(:node p) (:channel p)])
                      excuse (and (seq hashes) (every? held hashes))
                      ps     (if excuse
                               (vec (remove #(= :pending-tlcs (first %)) (:problems p)))
                               (:problems p))]
               :when (seq ps)]
           (assoc p :problems ps)))))

(defn channel-problems
  "Problems with the channels opened during setup, except those in `skip`
  (channels a workload closed on purpose)."
  ([initial final]
   (channel-problems initial final #{}))
  ([initial final skip]
   (let [initial (by-channel initial)
         final   (by-channel final)]
     (vec
       (for [[id views] initial
             :when (not (skip id))
             [node init] views
             :let [ch       (get-in final [id node])
                   peer     (first (remove #{node} (keys views)))
                   peer-ch  (get-in final [id peer])
                   problems (if (nil? ch)
                              [[:missing]]
                              (cond-> []
                                (not= "channelready" (state-name ch))
                                (conj [:not-ready (get-in ch [:state :state_name])])

                                (seq (:pending_tlcs ch))
                                (conj [:pending-tlcs (count (:pending_tlcs ch))])

                                (not= (total init) (total ch))
                                (conj [:conservation {:initial (total init), :final (total ch)}])

                                (and peer-ch
                                     (not= (amount ch :local_balance)
                                           (amount peer-ch :remote_balance)))
                                (conj [:mirror {:local       (amount ch :local_balance)
                                                :peer-remote (amount peer-ch :remote_balance)}])))]
             :when (seq problems)]
         {:channel id, :node node, :peer peer, :problems problems})))))

(defn channel-summary
  "Counts channel ends in the final read: ready with nothing pending, ready but still
  holding TLCs after the quiet period (wedged), and no longer ready (closed or shutting
  down, e.g. force-closed by a node)."
  [channels-by-node]
  (frequencies
    (for [[_node channels] channels-by-node
          ch               channels]
      (cond (not= "channelready" (state-name ch)) :not-ready
            (seq (:pending_tlcs ch))              :ready-with-pending-tlcs
            :else                                 :ready-clean))))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [read (->> history
                      (filter #(and (= :ok (:type %)) (= :read (:f %))))
                      last
                      :value)]
        (if-not read
          {:valid? :unknown, :error "no final read"}
          (let [closed     (closed-on-purpose history)
                held       (held-on-chain (:channels read) closed)
                channels   (-> (channel-problems @(:initial-channels test) (:channels read) closed)
                               (excuse-held (:channels read) held))
                statuses   (into {} (map (juxt :hash :status)) (:payments read))
                unfinished (vec (remove #(or (#{"Success" "Failed"} (:status %)) (held (:hash %)))
                                        (:payments read)))
                client-ok  (->> history
                                (filter #(and (= :ok (:type %)) (= :pay (:f %))))
                                (map #(get-in % [:value :hash])))
                flipped    (vec (for [h client-ok
                                      :when (not= "Success" (statuses h))]
                                  {:hash h, :final-status (statuses h)}))
                probe      (->> history
                                (filter #(and (= :ok (:type %)) (= :probe (:f %))))
                                last
                                :value
                                (mapv #(assoc % :sender-balance
                                              (sender-balance (:channels read) (:from %) (:to %)))))
                ; A direction whose sender has spent its balance can't pay, which isn't a fault.
                ; But Fiber's router reports the same "Insufficient balance" error, with liquidity
                ; 0, for a channel it can't use at all (offline, closed or stalled), so the error
                ; alone proves nothing: the sender's balance must also be below the probe amount.
                ; Histories recorded before probes carried :amount used 1000000.
                no-funds?  #(and (some-> (:error %) (str/includes? "Insufficient balance"))
                                 (some-> (:sender-balance %) (< (or (:amount %) 1000000))))
                probe-bad  (vec (remove #(or (= "Success" (:status %)) (no-funds? %)) probe))]
            (cond->
              {:valid?              (and (empty? channels) (empty? unfinished) (empty? flipped)
                                         (empty? probe-bad))
               :channel-summary     (channel-summary (:channels read))
               :payments            (count (:payments read))
               :payments-ok         (count client-ok)
               :channel-problems    channels
               :unfinished-payments unfinished
               :success-flipped     flipped
               :probe-failures      probe-bad}
              (seq closed) (assoc :closed-on-purpose (count closed)
                                  :held-on-chain     (vec (filter #(and (held (:hash %))
                                                                        (not (#{"Success" "Failed"} (:status %))))
                                                                  (:payments read)))))))))))

(defn close-checker
  "With --close, every channel is asked to close cooperatively halfway through the
  faults. Valid when every end of every channel opened during setup is Closed in the
  final read; reports the final states with their flags (cooperative or not)."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [read    (->> history
                         (filter #(and (= :ok (:type %)) (= :read (:f %))))
                         last
                         :value)
            request (->> history
                         (filter #(and (= :close (:f %)) (= :info (:type %)) (:value %)))
                         last
                         :value)]
        (if-not read
          {:valid? :unknown, :error "no final read"}
          (let [ids  (set (for [[_ chs] @(:initial-channels test) ch chs] (:channel_id ch)))
                ends (for [[node chs] (:channels read)
                           ch         chs
                           :when      (ids (:channel_id ch))]
                       {:node node, :channel (:channel_id ch), :state (:state ch)})
                open (vec (remove #(= "closed" (state-name {:state (:state %)})) ends))]
            {:valid?         (and (seq ends) (empty? open))
             :close-requests request
             :final-states   (frequencies (map :state ends))
             :not-closed     open}))))))
