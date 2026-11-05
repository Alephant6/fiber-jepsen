(ns jepsen.fiber.faults.disconnect
  "The disconnect fault. It closes the p2p connection between two neighbouring nodes
  with fnn's disconnect_peer RPC, and a later :reconnect op connects every pair again
  with connect_peer, so channels reestablish while both processes keep running.
  Partitions rarely get there: TCP keeps the connection open through an iptables drop
  and retransmits once it heals, so no reestablishment happens.

  With --disconnect-flash MIN-MAX, each fault is a flash disconnect instead: one
  :flash-disconnect op disconnects the pair, waits a drawn MIN-MAX milliseconds and
  connects it again (C-05's network blips of 100 ms to 5 s, as a real disconnect:
  packet loss that short never breaks TCP)."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [random :as rand]
                    [util :as util]]
            [jepsen.fiber [rpc :as rpc]
                          [schedule :as schedule]
                          [topology :as topology]])
  (:import (java.util SplittableRandom)))

(defn- try-call
  "Calls the RPC and returns its result, or a short failure string: the node may be
  down when this fault is combined with kills or pauses."
  [node method params]
  (try (or (rpc/call node method params) :ok)
       (catch Exception e
         (warn "disconnect nemesis:" node method (.getMessage e))
         (str "failed: " (.getMessage e)))))

(defn- connect!
  "connect_peer from `a` to `b`, at the address in `b`'s node_info `info`, or a
  short failure string. The address can be missing when node_info failed (b
  down or paused, with other faults), and then the op records that rather than
  throwing: a thrown :reconnect fails the run's :faults check, though nothing
  is wrong with the fault."
  [a b info]
  (let [addr (try (topology/p2p-address b info)
                  (catch Exception e (str "failed: " (.getMessage e))))]
    (if (str/starts-with? addr "failed")
      addr
      (try-call a "connect_peer" {:address addr}))))

(defn disconnect-nemesis
  []
  (reify
    n/Reflection
    (fs [_] #{:disconnect :reconnect :flash-disconnect})

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

          :flash-disconnect
          (let [{[a b] :edge, down-ms :down-ms} (:value op)
                ; Fetched first, so the reconnect isn't later than drawn.
                info (try (rpc/call b "node_info") (catch Exception _ nil))
                down (try-call a "disconnect_peer" {:pubkey (pubkeys b)})
                _    (Thread/sleep (long down-ms))
                ; Whether a still saw b as a peer when the wait ended: true means the
                ; connection came back before the reconnect, or never went away.
                seen (try (boolean (some #(= (pubkeys b) (:pubkey %))
                                         (:peers (rpc/call a "list_peers"))))
                          (catch Exception e (str "failed: " (.getMessage e))))
                up   (connect! a b info)]
            (assoc op :value {:from a, :to b, :down-ms down-ms, :disconnect down
                              :peer-before-reconnect seen, :reconnect up}))

          :reconnect
          ; Every pair at once: each call to a paused node waits for its timeout,
          ; and in a row they held up the other faults' ops for tens of seconds.
          (assoc op :value
                 (into (sorted-map)
                       (util/real-pmap
                         (fn [[a b]]
                           (let [info (try (rpc/call b "node_info") (catch Exception _ nil))]
                             [(str a "->" b) (connect! a b info)]))
                         edges))))))

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
                         :color "#A0C4E9"}
                        ; A flash is one op: lines at its start and its end.
                        {:name  "flash disconnect"
                         :start #{}
                         :stop  #{}
                         :fs    #{:flash-disconnect}
                         :color "#5B8FC9"}}}))

(defn ops
  "Endless pairs of a :disconnect of one random channel's peers and a :reconnect.
  With :disconnect-flash [min max] (ms), endless :flash-disconnect ops instead, each
  of one random channel's peers for a time drawn uniformly from that range."
  [^SplittableRandom r opts]
  (let [edges (vec (topology/edges opts))]
    (if-let [[lo hi] (:disconnect-flash opts)]
      (repeatedly (fn []
                    (let [edge (schedule/pick r edges)]
                      {:type  :info
                       :f     :flash-disconnect
                       :value {:edge edge, :down-ms (+ lo (.nextInt r (int (inc (- hi lo)))))}})))
      (mapcat (fn [edge]
                [{:type :info, :f :disconnect, :value edge}
                 {:type :info, :f :reconnect}])
              (repeatedly #(schedule/pick r edges))))))

(defn effect
  "Whether a :disconnect or :flash-disconnect completion took effect (see
  jepsen.fiber.faults): not when its disconnect_peer call failed, which it
  records as a \"failed: ...\" string rather than throwing, since the node may
  be down when this fault runs with kills or pauses."
  [op]
  (let [r (case (:f op)
            :disconnect       (get-in op [:value :result])
            :flash-disconnect (get-in op [:value :disconnect])
            nil)]
    (when (and (string? r) (str/starts-with? r "failed"))
      (str "disconnect_peer " r))))

(def fault
  {:faults  #{:disconnect}
   :stream  :disconnect
   :package package
   :ops     ops
   :fired   {:disconnect #{:disconnect :flash-disconnect}}
   :effect  effect})
