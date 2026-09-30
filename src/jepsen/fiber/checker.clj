(ns jepsen.fiber.checker
  "Invariants checked against the final read, which happens after every fault
  has been healed and the network has had time to converge:

  - every channel opened during setup is still ChannelReady;
  - no channel still holds pending TLCs;
  - both ends of a channel agree on its balances (A's local = B's remote);
  - each channel's total (local + remote) equals its total at the start of the
    test: payments move money within a channel, they never create or destroy it;
  - every payment reached Success or Failed;
  - no payment the client saw succeed later reports a different status;
  - a fresh payment each way over every channel (the probe) succeeds: the
    network can still move money once faults are gone."
  (:require [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.fiber.rpc :as rpc]))

(defn state-name
  [channel]
  (-> channel :state :state_name str (str/replace "_" "") str/lower-case))

(defn amount
  [channel k]
  (or (rpc/hex->int (get channel k)) 0))

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

(defn channel-problems
  [initial final]
  (let [initial (by-channel initial)
        final   (by-channel final)]
    (vec
      (for [[id views] initial
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
        {:channel id, :node node, :peer peer, :problems problems}))))

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
          (let [channels   (channel-problems @(:initial-channels test) (:channels read))
                statuses   (into {} (map (juxt :hash :status)) (:payments read))
                unfinished (vec (remove #(#{"Success" "Failed"} (:status %)) (:payments read)))
                client-ok  (->> history
                                (filter #(and (= :ok (:type %)) (= :pay (:f %))))
                                (map #(get-in % [:value :hash])))
                flipped    (vec (for [h client-ok
                                      :when (not= "Success" (statuses h))]
                                  {:hash h, :final-status (statuses h)}))
                probe      (->> history
                                (filter #(and (= :ok (:type %)) (= :probe (:f %))))
                                last
                                :value)
                ; A direction whose sender has spent its balance can't pay, which isn't a fault.
                no-funds?  #(some-> (:error %) (str/includes? "Insufficient balance"))
                probe-bad  (vec (remove #(or (= "Success" (:status %)) (no-funds? %)) probe))]
            {:valid?              (and (empty? channels) (empty? unfinished) (empty? flipped)
                                       (empty? probe-bad))
             :channel-summary     (channel-summary (:channels read))
             :payments            (count (:payments read))
             :payments-ok         (count client-ok)
             :channel-problems    channels
             :unfinished-payments unfinished
             :success-flipped     flipped
             :probe-failures      probe-bad}))))))
