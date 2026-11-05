(ns jepsen.fiber.faults.power
  "Power loss (open question 3 of the chaos-test plan): a node loses power, so
  fnn dies at once and whatever it wrote but didn't fsync is gone; later power
  comes back and fnn starts again.

  With this fault, jepsen.fiber.db mounts each node's store on LazyFS
  (jepsen.lazyfs), a FUSE filesystem that keeps written data in a page cache of
  its own until it is fsynced. A :power-off op SIGKILLs fnn on its nodes and
  tells LazyFS to drop every write that wasn't fsynced; a :power-on op starts
  fnn again. LazyFS is built into the node image, and FUSE allowed in the node
  containers, only when scripts/run.sh sees `power` in its arguments
  (docker/docker-compose.lazyfs.yml).

  Fiber #1703 (in Fiber from 4777d5d6) added fiber.store_sync_writes: on, every
  store write waits for the disk; off, the default, a write returns once it is
  in the page cache, so a power loss can bring a node back with an older
  channel state. Run this fault with --store-sync-writes true and with false.
  The power checker reports the mode each node's fnn logged at start, how many
  power losses each node had and how many bytes of its store each dropped, and
  fails when a power-on didn't bring fnn back or an op threw. Lost state shows
  in the usual checks: channels, the ledger, the health checker."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [control :as c]
                    [db :as db]
                    [nemesis :as n]
                    [store :as store]]
            [jepsen.lazyfs :as lazyfs]
            [jepsen.fiber [db :as fdb]
                          [health :as health]
                          [schedule :as schedule]]
            [slingshot.slingshot :refer [try+]])
  (:import (java.util SplittableRandom)))

(def target-specs
  #{:one :all})

(def settle-ms
  "How long a :power-off waits after asking LazyFS to drop unsynced writes before
  it measures the store: LazyFS reads the request from its fifo on a thread of
  its own."
  1000)

(defn store-bytes
  "The apparent size in bytes of `node`'s store, as fnn sees it through LazyFS."
  [node]
  (try+ (some-> (c/su (c/exec :du :-sb (fdb/store-dir node))) (str/split #"\s+") first parse-long)
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn check-mounted!
  "Throws unless `node`'s store is a LazyFS mount."
  [node]
  (when-not (c/su (lazyfs/mounted? (fdb/store-lazyfs node)))
    (throw (ex-info (str "--faults power needs fnn's store on " node " to be a LazyFS mount;"
                         " scripts/run.sh builds the node image with LazyFS when its arguments"
                         " mention power")
                    {:type :store-not-on-lazyfs, :node node}))))

(defn power-off!
  "SIGKILLs fnn on `node`, then drops the store's unsynced writes. Returns the
  store's size before and after."
  [db test node]
  (db/kill! db test node)
  (let [before (store-bytes node)]
    (c/su (lazyfs/lose-unfsynced-writes! (fdb/store-lazyfs node)))
    (Thread/sleep settle-ms)
    {:store-bytes-before before
     :store-bytes-after  (store-bytes node)}))

(defn power-on!
  "Starts fnn on `node` and waits for its RPC: :started, or why it didn't come
  back."
  [db test node]
  (db/start! db test node)
  (try (fdb/await-rpc! node)
       :started
       (catch Exception e
         (str "RPC didn't come back: " (or (.getMessage e) (.getSimpleName (class e)))))))

(defn power-nemesis
  [db]
  (let [off (atom (sorted-set))]
    (reify
      n/Reflection
      (fs [_] #{:power-off :power-on})

      n/Nemesis
      (setup! [this test]
        (c/on-nodes test (fn [_ node] (check-mounted! node)))
        this)

      (invoke! [_ test op]
        (case (:f op)
          :power-off
          (assoc op :value
                 (into (sorted-map)
                       (c/on-nodes test (:nodes (:value op))
                                   (fn [test node]
                                     (let [r (power-off! db test node)]
                                       (swap! off conj node)
                                       r)))))

          :power-on
          (let [nodes (vec @off)]
            (assoc op :value
                   (into (sorted-map)
                         (when (seq nodes)
                           (c/on-nodes test nodes
                                       (fn [test node]
                                         (let [r (power-on! db test node)]
                                           (swap! off disj node)
                                           r)))))))))

      (teardown! [_ _test]))))

(defn package
  [opts]
  {:generator       nil
   :final-generator (when ((:faults opts) :power) {:type :info, :f :power-on, :value nil})
   :nemesis         (power-nemesis (:db opts))
   :perf            #{{:name  "power off"
                       :start #{:power-off}
                       :stop  #{:power-on}
                       :color "#E9A0A0"}}})

(defn ops
  "Endless pairs of a :power-off, its targets drawn from `r` (:power-targets:
  one node or all of them), and a :power-on."
  [^SplittableRandom r opts]
  (let [nodes (vec (:nodes opts))
        specs (vec (:power-targets opts [:one]))]
    (mapcat (fn [targets]
              [{:type :info, :f :power-off, :value {:nodes targets}}
               {:type :info, :f :power-on, :value nil}])
            (repeatedly #(case (schedule/pick r specs)
                           :one [(schedule/pick r nodes)]
                           :all nodes)))))

;; The checker

(def ^:private ansi
  #"\u001b\[[0-9;]*m")

(defn sync-modes
  "The store sync modes fnn logged at each start (\"Store sync writes: true\",
  Fiber #1703), in order. Empty for a build that doesn't log it."
  [lines]
  (vec (for [line  lines
             :let  [m (re-find #"Store sync writes: (true|false)" (str/replace line ansi ""))]
             :when m]
         (= "true" (second m)))))

(defn- per-node
  "A :power-off or :power-on completion's {node result}; nil for an invocation."
  [op]
  (let [v (:value op)]
    (when (and (map? v) (every? string? (keys v)))
      v)))

(defn check
  "`modes` is node -> sync-modes of its log (nil when the log is missing)."
  [history modes]
  (let [offs     (for [op history, :when (= :power-off (:f op)), [node r] (per-node op)]
                   (assoc r :node node, :time (:time op)))
        ons      (for [op history, :when (= :power-on (:f op)), [node r] (per-node op)]
                   {:node node, :time (:time op), :result r})
        dropped  (fn [{:keys [store-bytes-before store-bytes-after]}]
                   (when (and store-bytes-before store-bytes-after)
                     (- store-bytes-before store-bytes-after)))
        failed   (vec (remove #(= :started (:result %)) ons))
        crashed  (vec (for [op history
                            :when (and (#{:power-off :power-on} (:f op)) (:exception op))]
                        (select-keys op [:f :time :error])))
        mode     (into (sorted-map)
                       (for [[node ms] modes]
                         [node (cond (nil? ms)   :no-log
                                     (empty? ms) :not-logged
                                     (apply = ms) (first ms)
                                     :else        (vec ms))]))]
    {:valid?            (cond (seq crashed) false
                              (seq failed)  false
                              (empty? offs) :unknown
                              :else         true)
     :store-sync-writes mode
     :power-offs        (into (sorted-map) (frequencies (map :node offs)))
     :bytes-dropped     (into (sorted-map)
                              (for [[node os] (group-by :node offs)]
                                [node (vec (keep dropped os))]))
     :not-back          failed
     :nemesis-errors    crashed}))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (check history
             (into {} (for [node (:nodes test)]
                        [node (let [f (store/path test (name node) health/log-name)]
                                (try (when (.exists (io/file f))
                                       (with-open [r (io/reader f)]
                                         (sync-modes (line-seq r))))
                                     (catch Exception e
                                       (warn e "Couldn't read" f)
                                       nil)))]))))))

(def fault
  {:faults  #{:power}
   :stream  :power
   :package package
   :ops     ops
   :checker (fn [_opts] {:power (checker)})})
