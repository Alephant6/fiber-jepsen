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

(defn start-fnn!
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile logfile
       :pidfile pidfile
       :chdir   (node-dir node)
       ; :fnn-env holds what faults add (jepsen.fiber.faults/fnn-env): libfaketime
       ; for the clock fault, say.
       :env     (merge {:FIBER_SECRET_KEY_PASSWORD (str "password" (node-id node))
                        :RUST_LOG                  (:log-level test "info")
                        :RUST_BACKTRACE            "1"
                        :FIBER_NONCE_ORACLE_FILE   nonce-file}
                       (:fnn-env test))}
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
    (c/su (c/exec :rm :-rf (str (node-dir node) "/fiber/store") logfile pidfile nonce-file))
    (start-fnn! test node)
    (await-rpc! node))

  (teardown! [_ _test node]
    (c/su (cu/stop-daemon! binary pidfile)
          (c/exec :rm :-rf (str (node-dir node) "/fiber/store"))))

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
  (log-files [_ _test _node]
    ; Jepsen skips files that don't exist, e.g. the nonce records of an uninstrumented fnn.
    {logfile    "fnn.log"
     nonce-file "nonce-oracle.log"}))

(defn db
  []
  (FiberDB.))
