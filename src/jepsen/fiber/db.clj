(ns jepsen.fiber.db
  "Runs fnn on each node. Node directories, keys and wallets are baked into the
  image from the devnet snapshot, so setup only wipes the Fiber store and starts
  the daemon. Implements Kill (SIGKILL, so crashes can land mid-write) and Pause
  (SIGSTOP/SIGCONT) for jepsen.nemesis.combined."
  (:require [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]
                    [util :refer [await-fn]]]
            [jepsen.control.util :as cu]
            [jepsen.lazyfs :as lazyfs]
            [jepsen.fiber [rpc :as rpc]
                          [topology :as topology]]
            [slingshot.slingshot :refer [try+]]))

(def binary "/usr/local/bin/fnn")
(def logfile "/fiber/fnn.log")
(def pidfile "/fiber/fnn.pid")
; MuSig2 nonce-use records, written only by fnn builds with the nonce-oracle feature
; (instrument/). Kept across restarts within a test; see jepsen.fiber.nonce.
(def nonce-file "/fiber/nonce-oracle.log")

(defn node-id
  "n1 -> \"1\""
  [node]
  (subs (name node) 1))

(defn node-dir
  [node]
  (str "/fiber/tests/nodes/" (node-id node)))

(def small-fs
  "A small tmpfs on every node (docker/docker-compose.yml). With --faults disk,
  fnn's store lives on it, and the disk fault fills it and frees it."
  "/fiber/small")

(defn store-dir
  [node]
  (str (node-dir node) "/fiber/store"))

(defn small-store?
  "Whether the test puts fnn's store on small-fs."
  [test]
  (boolean (some #{:disk} (:faults test))))

(defn lazyfs-store?
  "Whether the test puts fnn's store on LazyFS (--faults power)."
  [test]
  (boolean (some #{:power} (:faults test))))

(defn store-lazyfs
  "The jepsen.lazyfs map of `node`'s store: the store directory is a FUSE mount
  that keeps written data in LazyFS's own page cache until it is fsynced, backed
  by /fiber/lazyfs/data. LazyFS is in the node image only when built for it
  (scripts/run.sh does with --faults power)."
  [node]
  (lazyfs/lazyfs {:dir        (store-dir node)
                  :lazyfs-dir "/fiber/lazyfs"
                  :cache-size "0.5GB"}))

(defn start-fnn!
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile logfile
       :pidfile pidfile
       :chdir   (node-dir node)
       :env     (cond-> {:FIBER_SECRET_KEY_PASSWORD (str "password" (node-id node))
                         :RUST_LOG                  (:log-level test "info")
                         :RUST_BACKTRACE            "1"
                         :FIBER_NONCE_ORACLE_FILE   nonce-file}
                  ; Fiber #1703; builds before it ignore the variable.
                  (some? (:store-sync-writes test))
                  (assoc :FIBER_STORE_SYNC_WRITES (str (:store-sync-writes test))))}
      binary
      :-d (node-dir node))))

(defn- fnn-pid
  "fnn's pid from its pidfile, or nil when there is none (kill! removes it)."
  []
  (try+ (str/trim (c/exec :cat pidfile))
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn- process-state
  "The ps state of process `pid`, e.g. \"Ssl\", or \"Tsl\" once stopped; nil when
  there is no such process."
  [pid]
  (try+ (not-empty (str/trim (c/exec :ps :-o "stat=" :-p pid)))
        (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn signal!
  "Sends `sig` (\"STOP\" or \"CONT\") to fnn by the pid in its pidfile. Returns
  fnn's ps state once the signal has taken effect, waiting up to a second, so the
  nemesis op shows what happened: T means stopped. :not-running when fnn isn't.

  This used to be cu/grepkill!, which on the node image stopped nothing:
  payments from and through the paused node went on succeeding. The cause wasn't
  found; its pipe (pgrep | xargs kill) exits 0 even when pgrep finds nothing or
  fails, so nothing reported it."
  [sig]
  (c/su
    (if-let [pid (fnn-pid)]
      (try+
        (c/exec :kill (str "-" sig) pid)
        (loop [tries 10]
          (let [state (process-state pid)]
            (if (or (nil? state)
                    (zero? tries)
                    (= (= "STOP" sig) (str/starts-with? state "T")))
              (or state :not-running)
              (do (Thread/sleep 100)
                  (recur (dec tries))))))
        (catch [:type :jepsen.control/nonzero-exit] _ :not-running))
      :not-running)))

(defn await-rpc!
  [node]
  (await-fn #(rpc/call node "node_info")
            {:log-message (str "Waiting for fnn RPC on " node)
             :timeout     120000}))

(defrecord FiberDB []
  db/DB
  (setup! [_ test node]
    (when (and (small-store? test) (lazyfs-store? test))
      (throw (ex-info "--faults disk and power each put fnn's store on a filesystem of their own; use one"
                      {:type :disk-and-power})))
    (c/su (c/exec :rm :-rf (store-dir node) logfile pidfile nonce-file)
          (when (small-store? test)
            ; The store directory becomes a link into small-fs.
            (c/exec :rm :-rf (str small-fs "/store") (str small-fs "/fill"))
            (c/exec :mkdir :-p (str small-fs "/store") (str (node-dir node) "/fiber"))
            (c/exec :ln :-s (str small-fs "/store") (store-dir node))))
    (when (lazyfs-store? test)
      (lazyfs/mount! (store-lazyfs node)))
    (start-fnn! test node)
    (await-rpc! node))

  (teardown! [_ test node]
    (c/su (cu/stop-daemon! binary pidfile))
    (when (lazyfs-store? test)
      (lazyfs/umount! (store-lazyfs node)))
    (c/su (c/exec :rm :-rf (store-dir node))))

  db/Primary
  (primaries [_ test]
    [(first (:nodes test))])

  (setup-primary! [_ test _node]
    (topology/setup! test))

  db/Kill
  (kill! [_ _test _node]
    (c/su (cu/stop-daemon! binary pidfile)))

  (start! [_ test node]
    (start-fnn! test node))

  db/Pause
  (pause! [_ _test _node]
    (signal! "STOP"))

  (resume! [_ _test _node]
    (signal! "CONT"))

  db/LogFiles
  (log-files [_ test node]
    ; Jepsen skips files that don't exist, e.g. the nonce records of an uninstrumented fnn.
    (cond-> {logfile    "fnn.log"
             nonce-file "nonce-oracle.log"}
      (lazyfs-store? test) (assoc (:log-file (store-lazyfs node)) "lazyfs.log"))))

(defn db
  []
  (FiberDB.))
