(ns jepsen.fiber.faults.cch-kill
  "The cch-kill fault: SIGKILL the standalone cross-chain hub (the fnn process on
  the cch host, jepsen.fiber.cch) and start it again, as kill does to fnn
  nodes. The hub's Fiber node and both LNDs keep running. CCH keeps its orders,
  and the intents of orders it is creating, in its own store and recovers them
  when it starts; meanwhile the hub's incoming payments stay held. Needs --cch."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.nemesis :as n]
            [jepsen.fiber.cch :as cch]))

(defn- try-op
  "(f), or a short failure string: the nemesis must not crash on an SSH error."
  [what f]
  (try (f)
       (catch Exception e
         (warn e "cch-kill nemesis:" what)
         (str "failed: " (.getMessage e)))))

(defn nemesis
  []
  (reify
    n/Reflection
    (fs [_] #{:kill-cch :start-cch})

    n/Nemesis
    (setup! [this _test] this)

    (invoke! [_ test op]
      (case (:f op)
        :kill-cch  (assoc op :value (try-op "kill" #(cch/on-host test cch/cch-host (cch/kill-cch!))))
        :start-cch (assoc op :value (try-op "start" #(cch/restart-cch! test)))))

    (teardown! [_ _test])))

(defn package
  [opts]
  (let [needed? ((:faults opts) :cch-kill)]
    {:generator       nil
     :final-generator (when needed? {:type :info, :f :start-cch})
     :nemesis         (nemesis)
     :perf            #{{:name  "cch"
                         :start #{:kill-cch}
                         :stop  #{:start-cch}
                         :color "#E9A0C8"}}}))

(defn ops
  "Endless kills of CCH, each followed by its restart. Nothing to draw: the gaps
  between them are the fault's randomness."
  [_r _opts]
  (cycle [{:type :info, :f :kill-cch}
          {:type :info, :f :start-cch}]))

(def fault
  {:faults  #{:cch-kill}
   :stream  :cch-kill
   :package package
   :ops     ops})
