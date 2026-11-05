(ns jepsen.fiber.onchain
  "With --onchain: checks that what each channel paid out on chain matches its
  last balances (assertion 6 of the chaos-test plan), and that each node's
  wallet changed by exactly what the channels' transactions explain.

  A node's wallet is the cells locked by its default funding lock script: Fiber
  funds channels from it, pays the node's share of a closed channel to it, and
  takes the fees of settlement transactions from it. At the start of the run
  (:chain-baseline) the test records every wallet's capacity. After the final
  read (:settle) it asks every channel that is still open to close cooperatively
  (and force-closes one that hasn't closed after a minute), waits until every
  close has settled on chain, and traces each channel from its funding cell: the
  transaction that spent it (the shutdown transaction, or a commitment
  transaction after a force close), then every transaction that spent a cell it
  created that isn't a wallet cell (the commitment cell, which the watchtowers
  settle after the commitment delay, in several steps when TLCs are pending).

  A party's receipt from a channel is what its wallet gained from those
  transactions: outputs to its lock minus inputs from its lock (a settlement
  transaction spends wallet cells to pay its fee and returns the change). Fiber
  pays each party its balance plus the CKB it reserved at opening (the occupied
  capacity of a cell plus 1 CKB for fees), less fees:

    cooperative close   balance + reserve - its shutdown fee
    force close         balance + reserve - the settlement fees it pays; the
                        party that settles last also pays the commitment fee
    pending TLCs        each goes to one side: to the receiver with the
                        preimage, back to the offerer once it has expired

  So a receipt must lie between balance + reserve - offered TLCs - every fee
  of the channel's transactions, and balance + reserve + received TLCs. Both
  parties use the default lock script, so they reserve the same amount, half of
  (funding capacity - local - remote). Balances and TLCs come from the party's
  own record of the closed channel.

  Invalid when a receipt is outside its range, when any other wallet received
  something from the channel, when a channel is still open or its cells are
  still unspent after the wait (:unknown instead while one of its TLCs hasn't
  expired: in a release build that takes hours), or when a wallet's change
  differs from what the channels' funding and closing transactions explain."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.fiber [chain :as chain]
                          [checker :as fc]
                          [client :as client]
                          [rpc :as rpc]]))

(def coop-wait-ms
  "How long the final close waits for cooperative closes before force-closing."
  60000)

(def ^:const poll-ms 3000)

(def max-txs
  "The most transactions traced per channel."
  64)

(def expiry-grace-ms
  "How long after a TLC expires its settlement may still be pending."
  60000)

;; Reading the chain

(defn parse-outpoint
  "A channel_outpoint as list_channels returns it (the 36-byte molecule OutPoint:
  tx hash, then the index as a little-endian u32) -> {:tx hash, :index n}."
  [s]
  (when (and (string? s) (= 74 (count s)))
    (let [h  (subs s 2)
          le (subs h 64 72)]
      {:tx    (str "0x" (subs h 0 64))
       :index (Long/parseLong (str/join (reverse (map str/join (partition 2 le)))) 16)})))

(defn- fetch-tx
  [cache hash]
  (or (get @cache hash)
      (let [res (chain/transaction hash)]
        (swap! cache assoc hash res)
        res)))

(defn- hex-long
  [s]
  (some-> s rpc/hex->int long))

(defn traced-tx
  "Transaction `hash` as the check needs it: its status and block, its inputs
  with the capacity and lock of the cells they spend, and its outputs. Outputs
  keep their :script, which tracing needs and strip removes."
  [cache hash]
  (let [res (fetch-tx cache hash)
        tx  (:transaction res)]
    {:hash    hash
     :status  (get-in res [:tx_status :status])
     :block   (hex-long (get-in res [:tx_status :block_number]))
     :inputs  (vec (for [in (:inputs tx)
                         :let [po (:previous_output in)
                               op {:tx (:tx_hash po), :index (hex-long (:index po))}
                               o  (get-in (fetch-tx cache (:tx op)) [:transaction :outputs (:index op)])]]
                     (assoc op
                            :capacity (hex-long (:capacity o))
                            :lock     (some-> (:lock o) chain/lock-key))))
     :outputs (vec (map-indexed (fn [i o]
                                  {:index    i
                                   :capacity (hex-long (:capacity o))
                                   :lock     (chain/lock-key (:lock o))
                                   :script   (:lock o)})
                                (:outputs tx)))}))

(defn- strip
  [tx]
  (update tx :outputs (partial mapv #(dissoc % :script))))

(defn- spends?
  [tx {:keys [tx index]}]
  (some #(and (= tx (:tx %)) (= index (:index %))) (:inputs tx)))

(defn- spender
  "The committed transaction that spent `cell` ({:tx :index :script}), or nil."
  [cache cell]
  (first (for [h     (distinct (for [o (chain/transactions-of (:script cell))
                                     :when (= "input" (:io_type o))]
                                 (:tx_hash o)))
               :let  [t (traced-tx cache h)]
               :when (spends? t cell)]
           t)))

(defn trace
  "Follows the channel funded at `outpoint` on chain: {:outpoint, :capacity of
  the funding cell, :funding (its transaction), :txs (the transactions that
  spent the funding cell and the non-wallet cells they created, in the order
  found), :live (non-wallet cells nobody has spent, the funding cell included if
  the channel never closed)}. `wallet?` tells wallet lock keys. Only committed
  transactions count. {:funding-status s} when the funding transaction isn't
  committed."
  [cache wallet? outpoint]
  (let [funding (traced-tx cache (:tx outpoint))]
    (if-not (= "committed" (:status funding))
      {:outpoint outpoint, :funding-status (:status funding)}
      (let [cell (get-in funding [:outputs (:index outpoint)])]
        (loop [frontier [(assoc cell :tx (:tx outpoint))]
               txs      []
               seen     #{}
               live     []]
          (if (or (empty? frontier) (<= max-txs (count txs)))
            {:outpoint   outpoint
             :capacity   (:capacity cell)
             :funding    (strip funding)
             :txs        (mapv strip txs)
             :live       (mapv #(dissoc % :script) (into live frontier))
             :truncated? (boolean (seq frontier))}
            (let [c (first frontier)
                  t (spender cache c)]
              (cond (nil? t)
                    (recur (rest frontier) txs seen (conj live c))

                    (seen (:hash t))
                    (recur (rest frontier) txs seen live)

                    :else
                    (recur (into (vec (rest frontier))
                                 (for [o (:outputs t) :when (not (wallet? (:lock o)))]
                                   (assoc o :tx (:hash t))))
                           (conj txs t)
                           (conj seen (:hash t))
                           live)))))))))

;; Pure accounting

(defn tx-nets
  "{lock-key net} for one transaction: outputs to each lock minus inputs from it."
  [{:keys [inputs outputs]}]
  (merge-with +
              (reduce (fn [m o] (update m (:lock o) (fnil + 0) (:capacity o))) {} outputs)
              (reduce (fn [m i] (update m (:lock i) (fnil - 0) (:capacity i))) {} inputs)))

(defn accounting
  "The fee paid by the transactions of a channel's trace, and {lock-key net}."
  [trace]
  (reduce (fn [acc tx]
            (-> acc
                (update :fee + (- (reduce + 0 (map :capacity (:inputs tx)))
                                  (reduce + 0 (map :capacity (:outputs tx)))))
                (update :net #(merge-with + % (tx-nets tx)))))
          {:fee 0, :net {}}
          (:txs trace)))

(defn party
  "One party's receipt from a settled channel against its own record of it."
  [capacity fee net record]
  (let [local    (fc/amount record :local_balance)
        remote   (fc/amount record :remote_balance)
        offered  (fc/amount record :offered_tlc_balance)
        received (fc/amount record :received_tlc_balance)
        reserved (- capacity local remote)
        reserve  (quot reserved 2)
        base     (+ local reserve)
        lo       (- base offered fee)
        hi       (+ base received)]
    (cond-> {:local    local
             :reserve  reserve
             :expected base
             :received net
             :diff     (- net base)
             :range    [lo hi]
             :ok?      (<= lo net hi)}
      (pos? offered)       (assoc :offered-tlcs offered)
      (pos? received)      (assoc :received-tlcs received)
      (odd? (long reserved)) (assoc :odd-reserve reserved))))

(defn- live-funding?
  [{:keys [outpoint live]}]
  (some #(and (= (:tx outpoint) (:tx %)) (= (:index outpoint) (:index %))) live))

(defn- awaiting-expiry?
  "Whether one of the channel's pending TLCs, in any end's record, expired less
  than expiry-grace-ms before `time` (or hasn't expired yet)."
  [ends time]
  (some (fn [tlc] (< time (+ (or (hex-long (:expiry tlc)) 0) expiry-grace-ms)))
        (mapcat :pending_tlcs (vals ends))))

(defn check-channel
  "The verdict for one channel: `ends` {node record}, its `trace`, `locks`
  {node lock-key} of every node, and the `time` (ms) the trace was read."
  [ends trace locks time]
  (let [summary (fn [m] (assoc m :states (into (sorted-map)
                                               (for [[n ch] ends]
                                                 [n (:state ch)]))))]
    (cond
      (nil? trace)
      (summary {:status :not-funded})

      (:error trace)
      (summary {:status :unknown, :error (:error trace)})

      (:funding-status trace)
      (summary {:status :not-funded, :funding-status (:funding-status trace)})

      (:truncated? trace)
      (summary {:status :unknown, :error (str "more than " max-txs " transactions")})

      (live-funding? trace)
      (summary {:status :open})

      (seq (:live trace))
      (summary {:status (if (awaiting-expiry? ends time) :awaiting-expiry :unsettled)
                :live   (:live trace)})

      :else
      (let [{:keys [fee net]} (accounting trace)
            parties  (into (sorted-map)
                           (for [[n ch] ends :when (get locks n)]
                             [n (party (:capacity trace) fee (get net (get locks n) 0) ch)]))
            ours     (set (for [n (keys ends)] (get locks n)))
            stray    (into (sorted-map)
                           (for [[n k] locks
                                 :when (and (not (ours k)) (not (zero? (get net k 0))))]
                             [n (get net k)]))]
        (summary (cond-> {:status   (cond (seq stray)                      :misdirected
                                          (every? :ok? (vals parties))     :ok
                                          :else                            :mismatch)
                          :capacity (:capacity trace)
                          :fee      fee
                          :txs      (count (:txs trace))
                          :parties  parties}
                   (seq stray) (assoc :stray stray)))))))

(defn wallet-check
  "Each node's wallet change from the baseline to the settle read, against the
  sum of the nets of every funding and closing transaction in between."
  [baseline settle]
  (let [txs (vals (into {} (for [[_ t] (:traces settle)
                                 tx    (cons (:funding t) (:txs t))
                                 :when tx]
                             [(:hash tx) tx])))]
    (into (sorted-map)
          (for [[node k] (:locks settle)
                :let [start (get-in baseline [:wallets node])
                      end   (get-in settle [:wallets node])]
                :when (and start end)]
            (let [in-range  (filter #(and (:block %) (< (:block start) (:block %) (inc (:block end))))
                                    txs)
                  explained (reduce + 0 (map #(get (tx-nets %) k 0) in-range))
                  change    (- (:capacity end) (:capacity start))]
              [node {:start     (:capacity start)
                     :end       (:capacity end)
                     :change    change
                     :explained explained
                     :txs       (count in-range)
                     :diff      (- change explained)}])))))

(defn analyze
  "The verdict from the :chain-baseline and :settle op values."
  [baseline settle]
  (cond
    (nil? settle)
    {:valid? :unknown, :error "no settle read"}

    (:error settle)
    {:valid? :unknown, :error (:error settle)}

    :else
    (let [ends     (fc/by-channel (:channels settle))
          channels (into (sorted-map)
                         (for [[id e] ends
                               :let [op (some (comp parse-outpoint :channel_outpoint) (vals e))]]
                           [id (check-channel e (when op (get (:traces settle) id))
                                              (:locks settle) (:time settle))]))
          by       (group-by (comp :status val) channels)
          wallets  (when baseline (wallet-check baseline settle))
          bad      (vec (for [[n w] wallets :when (not (zero? (:diff w)))] n))
          failed   (select-keys by [:open :unsettled :mismatch :misdirected])
          pending  (select-keys by [:awaiting-expiry :unknown])]
      {:valid?         (cond (or (seq failed) (seq bad))     false
                             (or (seq pending) (nil? wallets)) :unknown
                             :else                           true)
       :statuses       (into (sorted-map) (update-vals by count))
       :settled-in-s   (:waited-s settle)
       :closes         (:closes settle)
       :problems       (into (sorted-map) (for [[_ cs] (merge failed pending), [id c] cs] [id c]))
       :channels       (into (sorted-map) (for [[id c] channels :when (= :ok (:status c))]
                                            [id (select-keys c [:fee :txs :parties])]))
       :wallets        wallets
       :unbalanced-wallets bad})))

;; Operations

(defn- node-locks
  [test]
  (into (sorted-map) (for [n (:nodes test)]
                       [n (:default_funding_lock_script (rpc/call n "node_info"))])))

(defn- wallets
  [locks]
  (into (sorted-map) (for [[n script] locks] [n (chain/capacity-of script)])))

(defn baseline!
  "Every node's wallet before the faults."
  [test]
  (let [locks (node-locks test)]
    {:locks   (update-vals locks chain/lock-key)
     :wallets (wallets locks)}))

(defn- read-channels
  [test]
  (into (sorted-map) (for [n (:nodes test)] [n (client/read-node n)])))

(defn- closed?
  [ch]
  (= "closed" (fc/state-name ch)))

(defn- open-channels
  "{channel-id {:opener node, :acceptor node}} for channels ready on some end."
  [channels]
  (into (sorted-map)
        (for [[id ends] (fc/by-channel channels)
              :when     (some #(= "channelready" (fc/state-name %)) (vals ends))]
          [id {:opener   (some (fn [[n ch]] (when-not (:is_acceptor ch) n)) ends)
               :acceptor (some (fn [[n ch]] (when (:is_acceptor ch) n)) ends)}])))

(defn- shutdown!
  [node id force?]
  (try (rpc/call node "shutdown_channel"
                 (if force?
                   {:channel_id id, :force true}
                   {:channel_id id, :fee_rate (rpc/int->hex 1000)})
                 {:timeout 10000})
       :ok
       (catch Exception e (or (ex-message e) (str (class e))))))

(defn- request-close!
  "Asks for one close, from the opener first and then alternating, up to 6 tries."
  [id {:keys [opener acceptor]} force?]
  (let [nodes (cycle (remove nil? [opener acceptor]))]
    (loop [tries 1, [node & more] nodes]
      (let [result (shutdown! node id force?)]
        (if (or (= :ok result) (<= 6 tries))
          {:by node, :result result, :tries tries}
          (do (Thread/sleep 1000)
              (recur (inc tries) more)))))))

(defn close-all!
  "Closes every channel that is ready on either end: cooperatively, and by force
  (from the opener) when it isn't Closed on both ends after coop-wait-ms."
  [test]
  (let [open     (open-channels (read-channels test))
        requests (into (sorted-map) (for [[id who] open] [id (request-close! id who false)]))
        deadline (+ (System/currentTimeMillis) coop-wait-ms)
        pending  (loop []
                   (let [ends    (fc/by-channel (read-channels test))
                         pending (remove #(every? closed? (vals (get ends %))) (keys open))]
                     (if (and (seq pending) (< (System/currentTimeMillis) deadline))
                       (do (Thread/sleep poll-ms) (recur))
                       (set pending))))]
    (vec (for [[id req] requests]
           (cond-> (assoc req :channel id)
             (pending id) (assoc :forced (request-close! id (get open id) true)))))))

(defn- settling?
  "Whether a funded channel end still waits for its close to settle on chain."
  [ch]
  (and (:channel_outpoint ch)
       (or (not (closed? ch))
           (str/includes? (str (get-in ch [:state :state_flags])) "WAITING_ONCHAIN_SETTLEMENT"))))

(def record-keys
  [:channel_id :channel_outpoint :is_acceptor :pubkey :state :local_balance :remote_balance
   :offered_tlc_balance :received_tlc_balance :pending_tlcs :latest_commitment_transaction_hash
   :shutdown_transaction_hash])

(defn- trace-all
  [channels wallet?]
  (let [cache (atom {})]
    (into (sorted-map)
          (for [[id ends] (fc/by-channel channels)
                :let  [op (some (comp parse-outpoint :channel_outpoint) (vals ends))]
                :when op]
            [id (try (trace cache wallet? op)
                     (catch Exception e
                       (warn e "Couldn't trace channel" id)
                       {:outpoint op, :error (or (ex-message e) (str (class e)))}))]))))

(defn- settled-trace?
  [t]
  (or (:funding-status t) (:error t) (empty? (:live t))))

(defn settle!
  "Closes every open channel, waits up to :settle-timeout seconds for the closes
  to settle on chain, and reads the channels, their transactions and the wallets."
  [test]
  (let [start    (System/currentTimeMillis)
        locks    (node-locks test)
        keys*    (update-vals locks chain/lock-key)
        wallet?  (set (vals keys*))
        closes   (close-all! test)
        deadline (+ start (* 1000 (:settle-timeout test 300)))
        timeout? #(< deadline (System/currentTimeMillis))]
    (info "Waiting for every channel to settle on chain")
    ; First until every funded channel end is Closed with nothing left to settle,
    ; then until no channel's cells are left unspent on chain.
    (let [channels (loop []
                     (let [channels (read-channels test)]
                       (if (or (not-any? settling? (mapcat val channels)) (timeout?))
                         channels
                         (do (Thread/sleep poll-ms) (recur)))))
          _        (loop []
                     (when-not (or (every? settled-trace? (vals (trace-all channels wallet?)))
                                   (timeout?))
                       (Thread/sleep (long (* 3 poll-ms)))
                       (recur)))
          channels (read-channels test)
          ; Wallets read before and after the trace that agree mean no
          ; transaction landed in between.
          read     (fn [] (let [w1 (wallets locks)
                                t  (trace-all channels wallet?)
                                w2 (wallets locks)]
                            [(= w1 w2) t w2]))
          [_ traces wallets] (or (first (filter first (repeatedly 3 read)))
                                 (read))]
      {:closes   closes
       :settled? (every? settled-trace? (vals traces))
       :waited-s (quot (- (System/currentTimeMillis) start) 1000)
       :time     (System/currentTimeMillis)
       :locks    keys*
       :channels (update-vals channels (partial mapv #(select-keys % record-keys)))
       :traces   traces
       :wallets  wallets})))

(defmethod client/invoke-op :chain-baseline
  [test _node op]
  (assoc op :type :ok, :value (baseline! test)))

(defmethod client/invoke-op :settle
  [test _node op]
  (assoc op :type :ok, :value (settle! test)))

(defn checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [value (fn [f] (->> history
                               (filter #(and (= :ok (:type %)) (= f (:f %))))
                               last
                               :value))]
        (analyze (value :chain-baseline) (value :settle))))))
