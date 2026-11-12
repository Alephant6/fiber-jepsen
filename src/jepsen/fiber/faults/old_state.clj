(ns jepsen.fiber.faults.old-state
  "The old-state fault (C-11 of the chaos-test plan, as amended in the comment on
  quake's gist): a node comes back from an old copy of its store, so that it
  holds commitments its peers have already revoked. Killing a watchtower when
  nobody cheats tests nothing; this gives the watchtowers someone to catch. The
  plan's right result: the restored node never broadcasts a revoked commitment,
  and if it does, its peer's watchtower punishes it on chain.

  A :save-state op stops fnn on its node with SIGSTOP, copies the store to
  save-dir/db (the layout fnn --restore reads), and resumes fnn: a copy taken as
  a filesystem or VM snapshot would take it, with no restart and no reconnect.
  Payments then move the node's channels on. A :restore-state op, --old-state-hold
  seconds later, crashes fnn (SIGKILL), puts the copy back and starts fnn again,
  in one of two ways (--old-state-restore):
    raw    the store directory is replaced with the copy: a disk or snapshot
           rollback, which Fiber can't tell from a normal restart;
    fiber  Fiber's own backup and restore: `fnn --restore` (store/restore.rs)
           of the newest backup fnn made itself before the crash (a RocksDB
           checkpoint under fiber/backups, which the store actor writes at
           start and about a minute after a channel opens or closes). It swaps
           the store for the checkpoint and marks every channel that was
           ChannelReady or ShuttingDown Stale, so that the node audits it with
           its peer on reestablish and refuses to force-close it. The save
           op's copy isn't used: `fnn --restore` of it left the node listing
           no channels in the first CI runs.
  The op records the node's channels before the crash and after the restart
  (state, balances, latest commitment transaction hash), the store's
  fingerprint saved, before and after the restore, and, after waiting for the
  peers to reconnect, each peer's view of those channels. With
  --old-state-force-close it then asks the restored node to force-close every
  channel it still has, as an operator who trusts the restored node would: in
  raw mode that broadcasts the revoked commitment the node holds.

  What Fiber does about the old state (ae6f7d34, channel.rs): a reestablish
  whose commitment numbers differ by more than one fails on both ends
  (\"reestablish channel message with invalid commitment numbers\"), and both
  stay reestablishing; the timeout that would force-close a channel stuck
  reestablishing (REESTABLISH_TIMEOUT, 5 min) is only checked when a
  CheckActiveChannel event comes, which only an ack wait schedules. A Stale
  channel's audit fails the same way (\"Audit Failed for Stale channel\"). The
  watchtower (watchtower/actor.rs, every watchtower_check_interval_seconds: 1 s
  on the devnet) builds a revocation transaction when it finds a commitment of
  its peer's on chain whose number it holds a revocation for (\"Found an old
  version commitment tx submitted by remote\").

  The :old-state checker reads the restores, those log lines in every node's
  fnn.log, and, with --onchain, how each of the restored node's channels ended
  on chain: closed cooperatively, by a commitment its peer then revoked
  (punished), or by a commitment that was settled. It fails a run where a
  restored node's channel paid out by its old balances rather than its peer's
  latest record (a revoked commitment that nobody punished), or where the
  revocation paid the restored node. The restored node's own records are older
  than the chain, so jepsen.fiber.onchain judges its channels by its peers'
  records (the op lists the node under :stale-store), and `excused` keeps the
  fiber checker from failing the run for what the restore made the node forget."
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [control :as c]
                    [db :as db]
                    [nemesis :as n]
                    [store :as store]]
            [jepsen.fiber [checker :as fc]
                          [db :as fdb]
                          [health :as health]
                          [onchain :as onchain]
                          [rpc :as rpc]
                          [schedule :as schedule]
                          [topology :as topology]]
            [jepsen.fiber.faults [process :as process]]
            [slingshot.slingshot :refer [try+]])
  (:import (java.util SplittableRandom)))

(def target-specs
  "--old-state-targets: one (any node), middle (a node that forwards payments)."
  #{:one :middle})

(def restore-modes
  "--old-state-restore values."
  #{:raw :fiber})

(def save-dir
  "Where a :save-state op keeps its copy of the store, on the node."
  "/fiber/old-state")

(defn saved-store
  []
  (str save-dir "/db"))

(def reconnect-wait-ms
  "How long a :restore-state op waits after the restarted fnn answers before it
  checks that fnn reconnected to its channels' peers: they reconnect and the two
  ends exchange reestablish messages in that time."
  15000)

(def reestablish-wait-ms
  "How long a :restore-state op waits after connecting a peer itself, before it
  reads the peers' views (and, with --old-state-force-close, closes)."
  5000)

(def rest-gap-s
  "The gap between a restore and the next save."
  schedule/max-gap-s)

;; On the node

(defn- ms-since
  [t0]
  (long (/ (- (System/nanoTime) t0) 1e6)))

(defn- sh
  "Runs `script` with sh -c as root on the current node; its trimmed output."
  [script]
  (str/trim (c/su (c/exec :sh :-c script))))

(defn fingerprint
  "An md5 over the names and contents of every file under `dir` on the current
  node, or nil when there is no such directory."
  [dir]
  (try+ (not-empty (sh (str "cd " (c/escape dir) " && find . -type f | LC_ALL=C sort"
                            " | xargs -r md5sum | md5sum | cut -d' ' -f1")))
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn dir-bytes
  [dir]
  (try+ (some-> (sh (str "du -sb " (c/escape dir) " | cut -f1")) parse-long)
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn channel-view
  "`node`'s open channels as list_channels reports them: {channel-id {:state
  :local :remote :commitment :peer}}, :commitment being the hash of the latest
  commitment transaction the node holds and :peer the remote pubkey. A plain
  map, not a sorted one: other code looks keys like :error up in op values, and
  a sorted map with string keys throws on a keyword lookup."
  [node]
  (into {}
        (for [ch (:channels (rpc/call node "list_channels" {} {:timeout 10000}))]
          [(:channel_id ch) {:state      (get-in ch [:state :state_name])
                             :local      (fc/amount ch :local_balance)
                             :remote     (fc/amount ch :remote_balance)
                             :commitment (:latest_commitment_transaction_hash ch)
                             :peer       (:pubkey ch)}])))

(defn- try-view
  [node]
  (try (channel-view node)
       (catch Exception e {:error (or (ex-message e) (str (class e)))})))

(defn copy-store!
  "Copies `node`'s store to save-dir/db while fnn is stopped by SIGSTOP, and lets
  it go on. Throws when there is no running fnn to stop."
  [node]
  (let [state (fdb/signal! "STOP")
        t0    (System/nanoTime)]
    (try
      (when-not (and (string? state) (str/starts-with? state "T"))
        (throw (ex-info (str "fnn on " node " was not stopped for the copy: " state)
                        {:type :not-stopped, :state state})))
      (c/su (c/exec :rm :-rf save-dir)
            (c/exec :mkdir :-p save-dir)
            (c/exec :cp :-a (fdb/store-dir node) (saved-store)))
      {:stopped-ms (ms-since t0)}
      (finally
        (fdb/signal! "CONT")))))

(defn backups-dir
  [node]
  (str (fdb/node-dir node) "/fiber/backups"))

(defn newest-backup
  "{:path :ms} of the newest backup fnn made of `node`'s store (its directory is
  named by the time in ms), or nil."
  [node]
  (when-let [ms (try+ (some-> (not-empty (sh (str "ls -1 " (c/escape (backups-dir node))
                                                  " | grep -E '^[0-9]+$' | sort -n | tail -1")))
                              parse-long)
                      (catch [:type :jepsen.control/nonzero-exit] _ nil))]
    {:path (str (backups-dir node) "/" ms), :ms ms}))

(defn restore-store!
  "Puts an old store back while fnn on `node` is down: the saved copy over the
  store directory (:raw), or fnn --restore of backup directory `backup`
  (:fiber), which leaves the store it replaced next to it as store.bak.<ms>;
  that is deleted. Returns what fnn --restore printed, last lines first stripped
  of colour, for :fiber."
  [node mode backup]
  (case mode
    :raw   (do (c/su (c/exec :rm :-rf (fdb/store-dir node))
                     (c/exec :cp :-a (saved-store) (fdb/store-dir node)))
               nil)
    :fiber (let [out (c/su (c/exec :env (str "FIBER_SECRET_KEY_PASSWORD=password" (fdb/node-id node))
                                   "RUST_LOG=info" fdb/binary :--restore backup
                                   :-d (fdb/node-dir node)))]
             (sh (str "rm -rf " (c/escape (fdb/store-dir node)) ".bak.*"))
             (->> (str/split-lines (str/replace out #"\u001b\[[0-9;]*m" ""))
                  (map str/trim)
                  (filter #(re-find #"(?i)restor|stale|error|fail" %))
                  (take-last 8)
                  vec))))

(defn force-close!
  "Asks `node` to force-close channel `id`: :ok, or the error."
  [node id]
  (try (rpc/call node "shutdown_channel" {:channel_id id, :force true} {:timeout 10000})
       :ok
       (catch Exception e (or (ex-message e) (str (class e))))))

(defn- connected?
  [node pubkey]
  (try (boolean (some #(= pubkey (:pubkey %))
                      (:peers (rpc/call node "list_peers" {} {:timeout 10000}))))
       (catch Exception _ false)))

(defn reconnect!
  "Connects each node in `peers` that fnn on `node` isn't connected to, from the
  peer's side, as an operator who restored a node would: {peer :by-itself |
  :connected | the error}. fnn dials its channels' peers when it starts
  (MaintainConnections, then every 20 min), at the addresses its store holds; a
  store saved before it learned a peer's address can't, and the peer doesn't
  dial back on its own."
  [test node peers]
  (let [pubkeys @(:pubkeys test)
        info    (delay (rpc/call node "node_info" {} {:timeout 10000}))]
    (into {}
          (for [p peers]
            [p (if (connected? node (get pubkeys p))
                 :by-itself
                 (try (rpc/call p "connect_peer" {:address (topology/p2p-address node @info)}
                                {:timeout 10000})
                      :connected
                      (catch Exception e (str "connect_peer: " (or (ex-message e) (class e))))))]))))

(defn view-peers
  "The nodes whose pubkeys `view`'s channels have as their peer."
  [test view]
  (let [by-key (set/map-invert @(:pubkeys test))]
    (vec (sort (distinct (keep (fn [[_ ch]] (by-key (:peer ch))) view))))))

(defn moved
  "Ids of the channels whose latest commitment differs between `before` and
  `after` (channel views): the restored node now holds an older one."
  [before after]
  (vec (for [[id a] after
             :let [b (get before id)]
             :when (and b (:commitment a) (not= (:commitment a) (:commitment b)))]
         id)))

(defn save!
  "A :save-state op's work on `node`: the channels as they were just before the
  copy, the copy, and its fingerprint and size."
  [node]
  (let [view (try-view node)
        copy (copy-store! node)]
    (merge {:channels view}
           copy
           {:fingerprint (fingerprint (saved-store))
            :bytes       (dir-bytes (saved-store))})))

(defn restore!
  "A :restore-state op's work on `node`, which `saved` (a save! result) copied;
  see the namespace doc. `down` is the nemesis's atom of nodes it took down."
  [db test node saved down]
  (let [mode    (:old-state-restore test :raw)
        before  (try-view node)
        backup  (when (= :fiber mode) (newest-backup node))
        _       (when (and (= :fiber mode) (nil? backup))
                  (throw (ex-info (str "no backup of " node "'s store to restore")
                                  {:type :no-backup, :node node})))
        _       (swap! down conj node)
        _       (swap! onchain/restored-nodes conj node)
        crashed (System/currentTimeMillis)
        _       (fdb/crash!)
        fp-pre  (fingerprint (fdb/store-dir node))
        printed (restore-store! node mode (:path backup))
        fp-post (fingerprint (fdb/store-dir node))
        _       (db/start! db test node)
        up      (try (fdb/await-rpc! node) :started
                     (catch Exception e (str "RPC didn't come back: " (ex-message e))))
        _       (swap! down disj node)
        after   (try-view node)
        _       (Thread/sleep reconnect-wait-ms)
        partners (if (:error before) [] (view-peers test before))
        links   (try (reconnect! test node partners)
                     (catch Exception e {:error (or (ex-message e) (str (class e)))}))
        _       (when (some #{:connected} (vals links)) (Thread/sleep reestablish-wait-ms))
        ids     (if (:error after) [] (keys after))
        peers   (into {}
                      (for [p (:nodes test) :when (not= p node)
                            :let [v (try-view p)
                                  v (if (:error v) v (select-keys v ids))]
                            :when (seq v)]
                        [p v]))
        later   (try-view node)
        closes  (when (and (:old-state-force-close test) (not (:error later)))
                  (into {} (for [[id _] later] [id (force-close! node id)])))]
    (cond-> {:node         node
             :mode         mode
             :saved        saved
             :before       before
             :after        after
             :later        later
             :peers        peers
             :reconnect    links
             :started      up
             :moved        (if (or (:error before) (:error after)) [] (moved before after))
             :fingerprints {:saved (:fingerprint saved), :before fp-pre, :restored fp-post}
             ; Read by jepsen.fiber.onchain: this node's records are now older than the chain.
             :stale-store  [node]}
      printed (assoc :restore-output printed)
      backup  (assoc :backup (assoc backup :age-ms (- crashed (:ms backup))))
      closes  (assoc :force-close closes))))

(defn nemesis
  [db]
  (let [saved (atom {})        ; node -> save! result
        down  (atom #{})]      ; nodes a restore crashed and hasn't started again
    (reify
      n/Reflection
      (fs [_] #{:save-state :restore-state :old-state-heal})

      n/Nemesis
      (setup! [this test]
        (reset! onchain/restored-nodes #{})
        (c/on-nodes test (fn [_ _] (c/su (c/exec :rm :-rf save-dir))))
        this)

      (invoke! [_ test op]
        (let [node (get-in op [:value :node])]
          (case (:f op)
            :save-state
            (let [r (c/on-nodes test [node] (fn [_ node] (save! node)))]
              (swap! saved assoc node (get r node))
              (assoc op :value (assoc (get r node) :node node)))

            :restore-state
            (if-let [s (get @saved node)]
              (let [r (c/on-nodes test [node] (fn [test node] (restore! db test node s down)))]
                (swap! saved dissoc node)
                (assoc op :value (get r node)))
              (assoc op :value {:node node, :error "nothing saved"}))

            :old-state-heal
            (let [nodes (vec @down)]
              (assoc op :value
                     (into {}
                           (when (seq nodes)
                             (c/on-nodes test nodes
                                         (fn [test node]
                                           (db/start! db test node)
                                           (fdb/await-rpc! node)
                                           (swap! down disj node)
                                           :started)))))))))

      (teardown! [_ test]
        (c/on-nodes test (fn [_ _] (c/su (c/exec :rm :-rf save-dir))))))))

(defn package
  [opts]
  {:generator       nil
   :final-generator (when ((:faults opts) :old-state) {:type :info, :f :old-state-heal, :value nil})
   :nemesis         (nemesis (:db opts))
   :perf            #{{:name  "old state"
                       :start #{}
                       :stop  #{}
                       :fs    #{:save-state :restore-state}
                       :color "#C4A0E9"}}})

(defn ops
  "A sleep of --old-state-after seconds, then pairs of a :save-state and a
  :restore-state of one node, drawn from `r` by --old-state-targets: endless, or
  --old-state-count of them."
  [^SplittableRandom r opts]
  (let [specs   (vec (:old-state-targets opts [:one]))
        nodes   (vec (:nodes opts))
        middles (process/middle-nodes opts)
        faults  (set (:faults opts))]
    (when (and (some #{:middle} specs) (empty? middles))
      (throw (ex-info (str "--old-state-targets middle: no node in topology "
                           (name (:topology opts :line)) " is on more than one channel")
                      {:type :no-middle-node, :nodes nodes})))
    (when (some faults [:disk :power])
      (throw (ex-info "--faults old-state can't run with disk or power: they move fnn's store"
                      {:type :conflicting-faults})))
    (cons {:type :sleep, :value (:old-state-after opts 20)}
          (mapcat (fn [node]
                    [{:type :info, :f :save-state, :value {:node node}}
                     {:type :info, :f :restore-state, :value {:node node}}])
                  (cond->> (repeatedly #(case (schedule/pick r specs)
                                          :one    (schedule/pick r nodes)
                                          :middle (schedule/pick r middles)))
                    (:old-state-count opts) (take (:old-state-count opts)))))))

(defn gaps
  "No gap after the opening sleep; then --old-state-hold seconds from a save to
  its restore, and rest-gap-s from a restore to the next save."
  [_streams opts]
  (cons 0 (cycle [(long (* 1e9 (:old-state-hold opts 40)))
                  (long (* 1e9 rest-gap-s))])))

(defn effect
  "Whether a :restore-state completion took effect: the store was put back (in
  raw mode, byte for byte the saved copy) and fnn came back holding an older
  commitment on at least one channel than it did before the crash."
  [op]
  (when (= :restore-state (:f op))
    (let [v (:value op)]
      (cond (not (map? v))           "no result"
            (:error v)               (:error v)
            (not= :started (:started v)) (str (:started v))
            (and (= :raw (:mode v))
                 (not= (get-in v [:fingerprints :saved]) (get-in v [:fingerprints :restored])))
            "the restored store differs from the saved copy"
            (empty? (:moved v))      "no channel's commitment moved on between the save and the restore"
            :else                    nil))))

;; The logs

(def ^:private ansi
  #"\u001b\[[0-9;]*m")

(defn- add-new
  [v x]
  (if (or (nil? x) (some #{x} v)) v (conj v x)))

(defn log-findings
  "What fnn logged about old state in `lines`: reestablish handshakes whose
  commitment numbers are more than one apart (the numbers as each end printed
  them: its own local and remote, then the peer's), failed reestablishes (V2
  channels' rejections with the peer's next numbers) and Stale audits, channels
  force-closed for reestablishing too long (REESTABLISH_TIMEOUT), revoked commitments the watchtower found, the revocation
  transactions it sent or failed to send, and channels whose closing commitment
  it couldn't settle because none of its settlement data matched the commitment
  on chain (watchtower/actor.rs, \"Cannot recover settlement snapshot\")."
  [lines]
  (reduce
    (fn [acc raw]
      (let [line (str/replace raw ansi "")]
        (cond
          (str/includes? line "invalid commitment numbers")
          (update acc :reestablish-failed inc)

          ; Fiber's V2 channels (develop, #1677) reject a reestablish whose
          ; numbers don't fit in channel_v2.rs; the line names the peer's.
          (re-find #"Incompatible V2 [a-zA-Z ]+history" line)
          (let [[_ why] (re-find #"Incompatible V2 ([a-zA-Z ]+) history" line)
                [_ c a] (re-find #"next_commitment_number: (\d+), next_ack_number: (\d+)" line)]
            (-> acc
                (update :reestablish-failed inc)
                (update :v2-rejected conj (cond-> {:why why}
                                            c (assoc :peer-next [(parse-long c) (parse-long a)])))))

          (str/includes? line "has been reestablishing too long")
          (update acc :reestablish-timeouts inc)

          :else
          (let [[_ a b c d] (re-find #"local_commitment_number \((\d+), (\d+)\) peer_commitment_number \((\d+) (\d+)\)" line)
                [_ sa sb sc sd] (re-find #"Audit Failed for Stale channel: Local\(L:(\d+), R:(\d+)\), Peer\(L:(\d+), R:(\d+)\)" line)]
            (cond
              a (let [[a b c d] (map parse-long [a b c d])]
                  (cond-> acc
                    (< 1 (max (abs (- c b)) (abs (- d a))))
                    (update :gaps conj {:local [a b], :peer [c d]})))
              sa (update acc :stale-audits-failed conj {:local [(parse-long sa) (parse-long sb)]
                                                        :peer  [(parse-long sc) (parse-long sd)]})
              (str/includes? line "Passive audit passed") (update acc :stale-audits-passed inc)
              (str/includes? line "Found an old version commitment tx submitted by remote")
              (update acc :revoked-found add-new (second (re-find #"remote: (0x[0-9a-f]+)" line)))
              (re-find #"Revocation tx: .* sent" line) (update acc :revocations-sent inc)
              ; The watchtower sends the same revocation every check until it
              ; confirms; the pool turning down a duplicate isn't a failure.
              (and (re-find #"Failed to (send|build) revocation tx" line)
                   (not (str/includes? line "PoolRejectedDuplicatedTransaction")))
              (update acc :revocations-failed inc)
              (str/includes? line "Cannot recover settlement snapshot for channel")
              (update acc :settlement-unrecoverable add-new
                      (second (re-find #"Hash256\((0x[0-9a-f]+)\)" line)))
              :else acc)))))
    {:reestablish-failed 0, :gaps [], :v2-rejected [], :reestablish-timeouts 0
     :stale-audits-failed [], :stale-audits-passed 0
     :revoked-found [], :revocations-sent 0, :revocations-failed 0
     :settlement-unrecoverable []}
    lines))

(defn- read-findings
  [test node]
  (let [f (store/path test (name node) health/log-name)]
    (try (when (.exists (io/file f))
           (with-open [r (io/reader f)]
             (log-findings (line-seq r))))
         (catch Exception e
           (warn e "Couldn't read" f)
           nil))))

;; On chain

(defn- receipts
  [trace locks]
  (let [{:keys [fee net]} (onchain/accounting trace)]
    {:fee fee
     :net (into {} (for [[n k] locks] [n (get net k 0)]))}))

(defn chain-outcome
  "How restored node `node`'s channel `id` ended on chain, from the :settle op's
  value: its trace, its peer's record of it (the latest state) and the restored
  node's record after the restart (`old`, a channel-view entry). Statuses:
    :open, :unsettled   no verdict yet
    :cooperative        a shutdown transaction
    :latest             a commitment, settled by the latest state
    :punished           a commitment the peer's watchtower revoked
    :revoked-unpunished a commitment settled by the restored node's old
                        balances where they differ from the latest: a revoked
                        commitment went through
    :wrong-punisher     a revocation that paid the restored node
    :indistinguishable  a settled commitment whose old and latest balances
                        agree (within the fees), so the payouts can't tell.
  `check` turns :unsettled into :restored-share-locked when the restored node
  logged that it couldn't settle the channel (its settlement data doesn't match
  the commitment on chain): its share stays in the commitment cell."
  [settle node id old]
  (let [trace   (get-in settle [:traces id])
        ends    (get (fc/by-channel (:channels settle)) id)
        peer    (first (remove #{node} (keys ends)))
        latest  (get ends peer)
        locks   (:locks settle)
        spender (first (:txs trace))
        revoke  (first (filter :revokes (:txs trace)))]
    (cond
      (or (nil? trace) (:error trace) (:funding-status trace) (nil? latest))
      {:status :unknown, :peer peer, :error (or (:error trace) (:funding-status trace) "no trace or no peer record")}

      (nil? spender)
      {:status :open, :peer peer}

      (seq (:live trace))
      {:status :unsettled, :peer peer, :live (count (:live trace))}

      :else
      (let [wallet?    (set (vals locks))
            commitment (when (and (= 1 (count (:outputs spender)))
                                  (not (wallet? (:lock (first (:outputs spender))))))
                         (first (:outputs spender)))
            {:keys [fee net]} (receipts trace (select-keys locks [node peer]))
            capacity   (:capacity trace)
            l-local    (fc/amount latest :local_balance)
            l-remote   (fc/amount latest :remote_balance)
            reserve    (quot (- capacity l-local l-remote) 2)
            tlcs       (+ (fc/amount latest :offered_tlc_balance) (fc/amount latest :received_tlc_balance))
            slack      (+ fee tlcs)
            near?      (fn [x y] (<= (abs (- x y)) slack))
            got        (get net node 0)
            by-latest  (+ l-remote reserve)
            by-old     (when old (+ (:local old) reserve))
            punisher   (when revoke
                         (some (fn [n] (when (= (get locks n) (:lock (first (:outputs revoke)))) n))
                               [node peer]))
            base       {:peer               peer
                        :received           net
                        :restored-expected  {:latest by-latest, :old by-old}
                        :fee                fee}]
        (cond
          (nil? commitment)
          (assoc base :status :cooperative)

          revoke
          (assoc base
                 :status            (if (= punisher peer) :punished :wrong-punisher)
                 :commitment-number (:commitment-number commitment)
                 :revoked-number    (:revokes revoke)
                 :revocation        (:hash revoke)
                 :punisher          punisher)

          :else
          (let [old?    (and by-old (near? got by-old))
                latest? (near? got by-latest)]
            (assoc base
                   :commitment-number (:commitment-number commitment)
                   :status (cond (and old? latest?) :indistinguishable
                                 old?               :revoked-unpunished
                                 :else              :latest))))))))

(defn check
  "The :old-state verdict from `history` and `findings` (node -> log-findings)."
  [history findings]
  (let [restores (vec (filter #(and (= :restore-state (:f %)) (map? (:value %))
                                    (contains? (:value %) :stale-store))
                              history))
        settle   (->> history
                      (filter #(and (= :ok (:type %)) (= :settle (:f %))))
                      last
                      :value)
        outcomes (into (sorted-map)
                       (for [op restores
                             :let [{:keys [node after]} (:value op)]
                             :when (map? after)
                             [id old] after
                             :when (not (:error after))]
                         [id (let [o (if settle
                                       (chain-outcome settle node id old)
                                       {:status :unknown, :error "no --onchain settle read"})]
                               (cond-> (assoc o :restored node)
                                 (and (= :unsettled (:status o))
                                      (some #{id} (get-in findings [node :settlement-unrecoverable])))
                                 (assoc :status :restored-share-locked)))]))
        by       (group-by (comp :status val) outcomes)
        bad      (select-keys by [:revoked-unpunished :wrong-punisher])
        pending  (select-keys by [:unknown :open :unsettled :indistinguishable
                                  :restored-share-locked])
        took     (filter #(nil? (effect %)) restores)]
    {:valid?    (cond (seq bad)                        false
                      (or (empty? took) (seq pending)) :unknown
                      :else                            true)
     :restores  (vec (for [op restores
                           :let [v (:value op)]]
                       (cond-> {:node        (:node v)
                                :mode        (:mode v)
                                :time        (:time op)
                                :took-effect (nil? (effect op))
                                :moved       (:moved v)
                                :reconnect   (:reconnect v)
                                :states      (when (map? (:later v))
                                               (into (sorted-map)
                                                     (for [[id ch] (:later v) :when (map? ch)]
                                                       [id (:state ch)])))}
                         (:force-close v) (assoc :force-close (:force-close v))
                         (:restore-output v) (assoc :restore-output (:restore-output v)))))
     :statuses  (into (sorted-map) (update-vals by count))
     :channels  outcomes
     :logs      (into (sorted-map)
                      (for [[node f] findings :when f]
                        [node (cond-> f
                                (< 5 (count (:gaps f))) (update :gaps #(vec (take 5 %)))
                                (< 5 (count (:v2-rejected f))) (update :v2-rejected #(vec (take 5 %))))]))}))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (check history (into {} (for [node (:nodes test)] [node (read-findings test node)]))))))

;; What the restore makes the fiber checker see

(defn restored
  "{node time} of every restore in `history` that put a store back: the time the
  node's save before it began (the copy is taken somewhere between the save's
  invocation and its completion). The node has forgotten everything after that."
  [history]
  (loop [[op & more] (seq history), began {}, saves {}, out {}]
    (if (nil? op)
      out
      (let [node (when (#{:save-state :restore-state} (:f op)) (get-in op [:value :node]))]
        (cond
          (nil? node)
          (recur more began saves out)

          (and (= :save-state (:f op)) (map? (:value op)) (:fingerprint (:value op)))
          (recur more began (assoc saves node (get began node (:time op))) out)

          (= :save-state (:f op))
          (recur more (assoc began node (:time op)) saves out)

          (and (= :restore-state (:f op)) (contains? (:value op) :stale-store))
          (recur more began saves (update out node #(or % (get saves node 0))))

          :else
          (recur more began saves out))))))

;; As jepsen.fiber.closes/final-read; a fault's namespace doesn't load that
;; checker.
(defn- final-read
  "The value of the last :ok :read in `history`."
  [history]
  (->> history
       (filter #(and (= :ok (:type %)) (= :read (:f %))))
       last
       :value))

(defn excuse
  "The fiber checker's result `r`, less what a restore explains: problems of a
  restored node's channels, on either end (the restored end's balances are old
  and its peer can't reestablish with it; --onchain judges how they end); its
  own payments that it reports unfinished (it lost their outcome) or that it no
  longer reports Success although the client saw them succeed after its save
  began; other nodes' unfinished payments whose TLC one of its channels still
  holds in the final read (the channel can't settle it off chain any more), and
  other channels' pending TLCs that all forward such payments; and probes from
  or to it. Lists what it excused under :old-state-excused."
  [r history]
  (let [nodes (restored history)]
    (if (or (empty? nodes) (not (contains? r :channel-problems)))
      r
      (let [chans    (set (for [op history
                                :when (and (= :restore-state (:f op)) (contains? (:value op) :stale-store))
                                :let [v (get-in op [:value :after])]
                                :when (map? v)
                                id (keys v)]
                            id))
            final    (:channels (final-read history))
            held     (set (for [[_ records] final
                                ch records
                                :when (chans (:channel_id ch))
                                tlc (fc/live-tlcs ch)]
                            (:payment_hash tlc)))
            ; A forwarder can't settle or fail its upstream TLC before the
            ; downstream one, on a restored node's channel, resolves.
            forwarded? (fn [{:keys [channel node problems]}]
                         (let [tlcs (some #(when (= channel (:channel_id %)) (fc/live-tlcs %))
                                          (get final node))]
                           (and (seq tlcs)
                                (every? #(= :pending-tlcs (first %)) problems)
                                (every? (comp held :payment_hash) tlcs))))
            senders  (into {} (for [op history
                                    :when (and (= :ok (:type op)) (= :pay (:f op)))]
                                [(get-in op [:value :hash]) [(get-in op [:value :from]) (:time op)]]))
            forgot?  (fn [{:keys [hash]}]
                       (let [[from t] (get senders hash)]
                         (and from (contains? nodes from) (<= (get nodes from) t))))
            split    (fn [pred xs] [(vec (remove pred xs)) (vec (filter pred xs))])
            [problems p-x]   (split #(or (chans (:channel %))
                                         (contains? nodes (:node %))
                                         (contains? nodes (:peer %))
                                         (forwarded? %))
                                    (:channel-problems r))
            [unfinished u-x] (split #(or (contains? nodes (:node %)) (held (:hash %)))
                                    (:unfinished-payments r))
            [flipped f-x]    (split forgot? (:success-flipped r))
            [probes pr-x]    (split #(or (contains? nodes (:from %)) (contains? nodes (:to %)))
                                    (:probe-failures r))]
        (assoc r
               :valid?              (and (empty? problems) (empty? unfinished)
                                         (empty? flipped) (empty? probes))
               :channel-problems    problems
               :unfinished-payments unfinished
               :success-flipped     flipped
               :probe-failures      probes
               :old-state-excused   {:restored            nodes
                                     :channel-problems    p-x
                                     :unfinished-payments u-x
                                     :success-flipped     f-x
                                     :probe-failures      pr-x})))))

(defn excused
  "Wraps fiber checker `inner` with `excuse`."
  [inner]
  (reify checker/Checker
    (check [_ test history opts]
      (excuse (checker/check inner test history opts) history))))

(def fault
  {:faults  #{:old-state}
   :stream  :old-state
   :package package
   :ops     ops
   :gaps    gaps
   :checker (fn [_opts] {:old-state (checker)})
   :fired   {:old-state #{:restore-state}}
   :effect  effect})
