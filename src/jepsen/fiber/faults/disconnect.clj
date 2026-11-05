(ns jepsen.fiber.faults.disconnect
  "The disconnect fault. It closes the p2p connection between two neighbouring nodes
  with fnn's disconnect_peer RPC, and a later :reconnect op connects every pair again
  with connect_peer, so channels reestablish while both processes keep running.
  Partitions rarely get there: TCP keeps the connection open through an iptables drop
  and retransmits once it heals, so no reestablishment happens."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [random :as rand]]
            [jepsen.fiber [rpc :as rpc]
                          [schedule :as schedule]
                          [topology :as topology]]))

(defn- try-call
  "Calls the RPC and returns its result, or a short failure string: the node may be
  down when this fault is combined with kills or pauses."
  [node method params]
  (try (or (rpc/call node method params) :ok)
       (catch Exception e
         (warn "disconnect nemesis:" node method (.getMessage e))
         (str "failed: " (.getMessage e)))))

(defn disconnect-nemesis
  []
  (reify
    n/Reflection
    (fs [_] #{:disconnect :reconnect})

    n/Nemesis
    (setup! [this _test] this)

    (invoke! [_ test op]
      (let [edges   (topology/edges test)
            pubkeys @(:pubkeys test)]
        (case (:f op)
          :disconnect
          (let [[a b] (or (:value op) (rand/nth edges))]
            (assoc op :value {:from a, :to b
                              :result (try-call a "disconnect_peer" {:pubkey (pubkeys b)})}))

          :reconnect
          (assoc op :value
                 (into (sorted-map)
                       (for [[a b] edges]
                         (let [info (try (rpc/call b "node_info") (catch Exception _ nil))]
                           [(str a "->" b)
                            (try-call a "connect_peer" {:address (topology/p2p-address b info)})])))))))

    (teardown! [_ _test])))

(defn package
  "A nemesis package for :disconnect, in the shape jepsen.nemesis.combined uses.
  A :disconnect op's :value names the pair [a b] to disconnect; this package's
  generator leaves it out, so the nemesis picks a random pair. fiber-test takes its
  :disconnect ops from `ops` instead, which names the pair."
  [opts]
  (let [needed? ((:faults opts) :disconnect)
        start   {:type :info, :f :disconnect}
        stop    {:type :info, :f :reconnect}]
    {:generator       (when needed?
                        (->> (gen/flip-flop (gen/repeat start) (gen/repeat stop))
                             (gen/stagger (:interval opts))))
     :final-generator (when needed? stop)
     :nemesis         (disconnect-nemesis)
     :perf            #{{:name  "disconnect"
                         :start #{:disconnect}
                         :stop  #{:reconnect}
                         :color "#A0C4E9"}}}))

(defn ops
  "Endless pairs of a :disconnect of one random channel's peers and a :reconnect."
  [r opts]
  (let [edges (vec (topology/edges opts))]
    (mapcat (fn [edge]
              [{:type :info, :f :disconnect, :value edge}
               {:type :info, :f :reconnect}])
            (repeatedly #(schedule/pick r edges)))))

(def fault
  {:faults  #{:disconnect}
   :stream  :disconnect
   :package package
   :ops     ops})
