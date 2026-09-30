(ns jepsen.fiber.db
  "Runs fnn on each node. Node directories, keys and wallets are baked into the
  image from the devnet snapshot, so setup only wipes the Fiber store and starts
  the daemon. Implements Kill (SIGKILL, so crashes can land mid-write) and Pause
  (SIGSTOP/SIGCONT) for jepsen.nemesis.combined."
  (:require [jepsen [control :as c]
                    [db :as db]
                    [util :refer [await-fn]]]
            [jepsen.control.util :as cu]
            [jepsen.fiber [rpc :as rpc]
                          [topology :as topology]]))

(def binary "/usr/local/bin/fnn")
(def logfile "/fiber/fnn.log")
(def pidfile "/fiber/fnn.pid")

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
       :env     {:FIBER_SECRET_KEY_PASSWORD (str "password" (node-id node))
                 :RUST_LOG                  (:log-level test "info")
                 :RUST_BACKTRACE            "1"}}
      binary
      :-d (node-dir node))))

(defn await-rpc!
  [node]
  (await-fn #(rpc/call node "node_info")
            {:log-message (str "Waiting for fnn RPC on " node)
             :timeout     120000}))

(defrecord FiberDB []
  db/DB
  (setup! [_ test node]
    (c/su (c/exec :rm :-rf (str (node-dir node) "/fiber/store") logfile pidfile))
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
    (c/su (cu/grepkill! :stop binary)))

  (resume! [_ _test _node]
    (c/su (cu/grepkill! :cont binary)))

  db/LogFiles
  (log-files [_ _test _node]
    [logfile]))

(defn db
  []
  (FiberDB.))
