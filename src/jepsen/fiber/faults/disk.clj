(ns jepsen.fiber.faults.disk
  "The disk fault (C-13 of the chaos-test plan): the filesystem that holds one
  node's store fills up, and later space is freed. The plan's assertion: the
  node reports an error or stops, recovers once space is freed, and loses no
  state.

  With this fault, jepsen.fiber.db puts each node's store on a small tmpfs
  (db/small-fs, 256 MiB; docker/docker-compose.yml). A :fill-disk op fills it
  with one file. RocksDB preallocates its write-ahead log, so a running fnn
  can go on writing into that space for a while on a full disk; half the fills,
  at random, therefore also crash fnn (SIGKILL) and start it again on the full
  disk, so that RocksDB must open the store and start a new log with no space.
  A :free-disk op, before deleting the file, records what the
  node was doing while full (a health sample: does node_info answer, does a
  store write fail?) and what fnn logged since the fill (space errors, panics).
  It then frees the space and samples again after `recovery-ms`, to see whether
  fnn recovered by itself. If it didn't, the op restarts fnn, as an operator
  would, so that the run goes on, and records that it had to.

  The checker classifies each full window: fnn :stopped (exited) or :errored
  (alive, and the store write failed with an answer) is what the plan allows;
  :hung (node_info answers, the store write gets no answer: alive but doing
  nothing) and :unresponsive (node_info doesn't answer) are not; :unaffected
  (the store write succeeded on a full disk) means the fault didn't bite. It
  also reports how the payments from or to the node in the window ended. After the space is
  freed, fnn must be healthy again, by itself or, if it had stopped, after a
  restart: a node that was running but not working after the space came back
  fails, and so does a fill or free that threw (the disk may have stayed full).
  Lost state shows in the usual checks (channels, ledger), and panics in the
  health checker."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [control :as c]
                    [db :as db]
                    [nemesis :as n]
                    [util :refer [await-fn]]]
            [jepsen.fiber [db :as fdb]
                          [health :as health]
                          [rpc :as rpc]
                          [schedule :as schedule]]
            [slingshot.slingshot :refer [try+]])
  (:import (java.util SplittableRandom)))

(def fill-file
  (str fdb/small-fs "/fill"))

(def max-small-fs-bytes
  "The disk fault refuses to fill a filesystem bigger than this: it must be the
  small tmpfs, not the host's disk."
  (* 2 1024 1024 1024))

(def recovery-ms
  "How long a :free-disk op waits after freeing space before it checks whether
  fnn recovered by itself."
  10000)

(defn- exec-or-nil
  [& args]
  (try+ (str/trim (apply c/exec args))
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn space
  "{:size bytes, :avail bytes, :fstype t} of the current node's small-fs."
  []
  (let [out          (c/exec :df :-B1 "--output=size,avail,fstype" fdb/small-fs)
        [size avail fstype] (str/split (str/trim (last (str/split-lines out))) #"\s+")]
    {:size (parse-long size), :avail (parse-long avail), :fstype fstype}))

(defn check-small-fs!
  "Throws unless the current node's small-fs is a small filesystem of its own
  that holds fnn's store, so that filling it can't fill the host's disk."
  [node]
  (let [{:keys [size fstype]} (space)
        mounted?              (exec-or-nil :findmnt :-n :-o "TARGET" fdb/small-fs)
        link                  (exec-or-nil :readlink (fdb/store-dir node))]
    (when-not (and mounted? size (< 0 size max-small-fs-bytes))
      (throw (ex-info (str "--faults disk needs " fdb/small-fs " to be a small filesystem of its own"
                           " (docker/docker-compose.yml mounts a tmpfs); it is " (pr-str [fstype size]))
                      {:type :no-small-fs, :node node})))
    (when-not (= (str fdb/small-fs "/store") link)
      (throw (ex-info (str "fnn's store on " node " isn't on " fdb/small-fs ": " (pr-str link))
                      {:type :store-not-on-small-fs, :node node})))))

(defn store-bytes
  "The current node's store on small-fs: {:apparent bytes, :allocated bytes}.
  RocksDB preallocates its write-ahead log (fallocate), so the store holds more
  than it has written, and writes into that space succeed on a full disk."
  []
  (let [du (fn [& flags]
             (some-> (apply exec-or-nil :du (concat flags [(str fdb/small-fs "/store")]))
                     (str/split #"\s+") first parse-long))]
    {:apparent (du :-sb), :allocated (du :-s :-B1)}))

(defn fill!
  "Fills the current node's small-fs: one file with every free byte, then any
  last bytes a page at a time. Returns the space before and after, and the
  store's size."
  []
  (c/su
    (let [before (space)]
      (or (exec-or-nil :fallocate :-l (:avail before) fill-file)
          (do (exec-or-nil :dd "if=/dev/zero" (str "of=" fill-file) "bs=1M" "status=none")
              (exec-or-nil :dd "if=/dev/zero" (str "of=" fill-file) "bs=4k" "oflag=append"
                           "conv=notrunc" "status=none")))
      {:avail-before (:avail before), :avail-after (:avail (space)), :store (store-bytes)})))

(defn log-lines
  "How many lines fnn's log has on the current node."
  []
  (or (some-> (exec-or-nil :wc :-l fdb/logfile) (str/split #"\s+") first parse-long) 0))

(defn since-fill
  "What fnn logged on the current node after line `from`: lines about running
  out of space, the first of them, and panics."
  [from]
  (let [lines (or (some-> (exec-or-nil :tail :-n (str "+" (inc from)) fdb/logfile) str/split-lines) [])
        space (filter #(re-find #"(?i)no space left|ENOSPC|disk full" %) lines)]
    (cond-> {:space-errors (count space)
             :panics       (count (filter #(str/includes? % "panicked at") lines))}
      (seq space) (assoc :first-space-error (let [l (str/trim (first space))]
                                              (subs l 0 (min 300 (count l))))))))

(defn- brief
  "The parts of a health sample a window's record keeps."
  [sample]
  (cond-> {:status (health/status sample)
           :state  (get-in sample [:process :state])}
    (get-in sample [:rpc :error])         (assoc :rpc-error (get-in sample [:rpc :error]))
    (get-in sample [:store-write :error]) (assoc :store-write-error (get-in sample [:store-write :error]))))

(defn restart!
  "Stops fnn if it is still running and starts it again, waiting for its RPC."
  [db test node]
  (db/kill! db test node)
  (db/start! db test node)
  (try (fdb/await-rpc! node) :ok
       (catch Exception e (str "RPC didn't come back: " (.getMessage e)))))

(def restart-full-ms
  "How long a restart on a full disk waits for fnn's RPC."
  20000)

(defn restart-full!
  "Crashes fnn (SIGKILL) on a full disk and starts it again, as a crash or an
  operator's restart would: RocksDB must then open the store, replay its log and
  start a new one, with no space. :rpc-up when fnn answered within
  restart-full-ms, or how it didn't."
  [db test node]
  (fdb/crash!)
  (db/start! db test node)
  (try (await-fn #(rpc/call node "node_info") {:timeout restart-full-ms, :retry-interval 500, :log-interval 60000})
       :rpc-up
       (catch Exception _
         (let [state (:state (health/process))]
           (if (or (= "gone" state) (str/starts-with? state "Z")) :exited :no-rpc)))))

(defn disk-nemesis
  [db]
  (let [filled (atom {})]   ; node -> {:log-from n}
    (reify
      n/Reflection
      (fs [_] #{:fill-disk :free-disk})

      n/Nemesis
      (setup! [this test]
        (c/on-nodes test (fn [_ node] (check-small-fs! node)))
        this)

      (invoke! [_ test op]
        (case (:f op)
          :fill-disk
          (assoc op :value
                 (into (sorted-map)
                       (c/on-nodes test (:nodes (:value op))
                                   (fn [test node]
                                     (let [from (log-lines)
                                           r    (fill!)]
                                       (swap! filled assoc node {:log-from from})
                                       (cond-> r
                                         (:restart (:value op))
                                         (assoc :restart (restart-full! db test node))))))))

          :free-disk
          (let [nodes (vec (keys @filled))]
            (assoc op :value
                   (into (sorted-map)
                         (when (seq nodes)
                           (c/on-nodes test nodes
                                       (fn [test node]
                                         (let [{:keys [log-from]} (@filled node)
                                               full     (health/sample! test node)
                                               logged   (since-fill log-from)]
                                           (c/su (c/exec :rm :-f fill-file))
                                           (swap! filled dissoc node)
                                           (Thread/sleep recovery-ms)
                                           (let [after (health/sample! test node)
                                                 ok?   (= :ok (health/status after))
                                                 rec   (merge {:while-full (brief full)
                                                               :after-free (brief after)}
                                                              logged)]
                                             (if ok?
                                               rec
                                               (let [res (restart! db test node)]
                                                 (assoc rec
                                                        :restarted     res
                                                        :after-restart (brief (health/sample! test node)))))))))))))))

      (teardown! [_ test]
        (c/on-nodes test (fn [_ _] (c/su (c/exec :rm :-f fill-file))))))))

(defn package
  [opts]
  {:generator       nil
   :final-generator (when ((:faults opts) :disk) {:type :info, :f :free-disk, :value nil})
   :nemesis         (disk-nemesis (:db opts))
   :perf            #{{:name  "disk full"
                       :start #{:fill-disk}
                       :stop  #{:free-disk}
                       :color "#A0E9C4"}}})

(defn ops
  "Endless pairs of a :fill-disk of one node drawn from `r` and a :free-disk.
  Half the fills, at random, also restart fnn on the full disk (:restart)."
  [^SplittableRandom r opts]
  (let [nodes (vec (:nodes opts))]
    (mapcat (fn [fill]
              [{:type :info, :f :fill-disk, :value fill}
               {:type :info, :f :free-disk, :value nil}])
            (repeatedly #(let [node (schedule/pick r nodes)]
                           {:nodes [node], :restart (.nextBoolean r)})))))

(defn while-full
  "How a node behaved while its disk was full, from a :free-disk record:
  :stopped (fnn exited), :errored (node_info answered and the store write
  failed with an answer, an error), :hung (node_info answered, the store write
  got no answer within health/rpc-timeout-ms: alive but doing nothing),
  :unresponsive (node_info didn't answer), :unaffected (the store write
  succeeded), or :unknown."
  [rec]
  (let [w (:while-full rec)]
    (case (:status w)
      :exited       :stopped
      :not-working  (if (= "SocketTimeoutException" (:store-write-error w)) :hung :errored)
      :unresponsive :unresponsive
      :ok           :unaffected
      :unknown)))

(defn after-free
  "How a node came back once space was freed: :by-itself, :after-restart (it had
  stopped, so a restart is what an operator would do), :not-working (it was
  running and still didn't work, so the op restarted it), or :not-recovered."
  [rec]
  (cond (= :ok (get-in rec [:after-free :status]))     :by-itself
        (not= :ok (get-in rec [:after-restart :status])) :not-recovered
        (= :stopped (while-full rec))                  :after-restart
        (= :exited (get-in rec [:after-free :status]))   :after-restart
        :else                                          :not-working))

(defn- pays-of
  [history node from to]
  (frequencies
    (for [op    history
          :when (and (= :pay (:f op)) (not= :invoke (:type op)) (<= from (:time op) to)
                     (or (= node (get-in op [:value :from])) (= node (get-in op [:value :to]))))]
      (:type op))))

(defn- per-node
  "A :fill-disk or :free-disk completion's {node record}; nil for an invocation."
  [op]
  (let [v (:value op)]
    (when (and (map? v) (every? string? (keys v)))
      v)))

(defn check
  [history]
  (let [fills   (atom {})
        windows (vec (for [op    history
                           :let  [_ (when (= :fill-disk (:f op))
                                      (doseq [[n r] (per-node op)]
                                        (swap! fills assoc n (assoc r :time (:time op)))))]
                           :when (= :free-disk (:f op))
                           [node rec] (per-node op)
                           :let  [fill (get @fills node)]]
                       (merge {:node node, :time (:time op)
                               :while-full (while-full rec), :after-free (after-free rec)
                               :payments (pays-of history node (:time fill 0) (:time op))}
                              (when (:restart fill) {:restarted-full (:restart fill)})
                              (when-let [b (get-in fill [:store :allocated])] {:store-allocated-bytes b})
                              (select-keys rec [:space-errors :first-space-error :panics]))))
        bad     (vec (filter #(or (#{:unresponsive :hung} (:while-full %))
                                  (#{:not-working :not-recovered} (:after-free %)))
                             windows))
        ; A fill or free that threw: a disk may have stayed full, or fnn down.
        crashed (vec (for [op history
                           :when (and (#{:fill-disk :free-disk} (:f op)) (:exception op))]
                       (select-keys op [:f :time :error])))]
    {:valid?         (cond (seq crashed)                                     false
                           (empty? windows)                                  :unknown
                           (seq bad)                                         false
                           (every? #(= :unaffected (:while-full %)) windows) :unknown
                           :else                                             true)
     :while-full     (frequencies (map :while-full windows))
     :after-free     (frequencies (map :after-free windows))
     :failures       bad
     :nemesis-errors crashed
     :windows        windows}))

(defn checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (check history))))

(def fault
  {:faults  #{:disk}
   :stream  :disk
   :package package
   :ops     ops
   :checker (fn [_opts] {:disk (checker)})
   :fired   {:disk #{:fill-disk}}})
