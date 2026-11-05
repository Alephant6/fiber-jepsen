(ns jepsen.fiber.topology
  "Builds the channel graph before the workload starts. The graph is chosen by
  name (--topology) and spans the test's nodes (--nodes, n1 to n6 in
  docker/docker-compose.yml). The default, line, is n1 - n2 - n3 of public
  channels, so payments between the ends are forwarded by the middle node; ring
  and mesh give payments more than one route. Records each node's starting
  channels for the conservation check."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.util :refer [await-fn]]
            [jepsen.fiber.rpc :as rpc]))

(def funding-ckb
  "CKB the opener puts into each channel. The acceptor adds its auto-accept amount."
  1000)

(def shannons-per-ckb 100000000)

(def known-peer-ids
  "Peer IDs of the devnet test keys (tests/bruno/environments/test.bru), used if
  node_info doesn't report a full multiaddr. The keys of n4 and up are made by
  scripts/make-snapshot.sh, and their nodes are reached by the address node_info
  reports."
  {"n1" "QmbvRjJHAQDmj3cgnUBGQ5zVnGxUKwb2qJygwNs2wk41h8"
   "n2" "QmSRcPqUn4aQrKHXyCDjGn2qBVf43tWBDS2Wj9QDUZXtZp"
   "n3" "QmaFDJb9CkMrXy7nhTWBY5y9mvuykre3EzzRsCJUAVXprZ"})

(defn line
  "n1 - n2 - ... - nk, in the order of `nodes`."
  [nodes]
  (map vec (partition 2 1 nodes)))

(defn ring
  "The line, closed by a channel from the last node to the first: two routes
  between any two nodes. Three nodes make a triangle; fewer, just the line."
  [nodes]
  (cond-> (vec (line nodes))
    (< 2 (count nodes)) (conj [(last nodes) (first nodes)])))

(defn mesh
  "The ring, plus a channel from each node in the first half to the node
  opposite it on the ring (i to i + k/2 for k nodes), where that isn't a ring
  channel already. Six nodes get nine channels, three per node; a node's
  neighbours are one hop away and the rest two, by three disjoint routes. Four
  nodes make a complete graph."
  [nodes]
  (let [nodes (vec nodes)
        k     (count nodes)
        ring  (ring nodes)
        has?  (set (mapcat (fn [[a b]] [[a b] [b a]]) ring))]
    (into ring
          (for [i     (range (quot k 2))
                :let  [edge [(nodes i) (nodes (+ i (quot k 2)))]]
                :when (and (< 3 k) (not (has? edge)))]
            edge))))

(def topologies
  "Channel graphs by name. Each takes the test's nodes and returns the channels to
  open, as [opener acceptor] pairs. Each pair has one channel, so a pair names it."
  {:line line
   :ring ring
   :mesh mesh})

(defn edges
  "The channels of the test (or test options) `test`: its :topology's graph over
  its :nodes."
  [test]
  ((topologies (:topology test :line)) (:nodes test)))

(defn neighbours
  "{node #{node ...}}: the nodes each node has a channel with, in graph `edges`."
  [edges]
  (reduce (fn [m [a b]] (-> m (update a (fnil conj #{}) b) (update b (fnil conj #{}) a)))
          {}
          edges))

(defn probe-pairs
  "[from to route?] for each payment of the post-heal probe: one each way over
  every channel, then, with :probe-routes, one from every node to every node it
  has no channel with (route? true), which only multi-hop routing can deliver."
  [test]
  (let [edges (edges test)
        nb    (neighbours edges)]
    (concat (for [[a b] edges, [from to] [[a b] [b a]]] [from to false])
            (when (:probe-routes test)
              (for [from (:nodes test), to (:nodes test)
                    :when (and (not= from to) (not (contains? (nb from) to)))]
                [from to true])))))

(defn node-ip
  "nN's address on the Docker network (docker/docker-compose.yml): 10.77.0.(10 + N)."
  [node]
  (str "10.77.0." (+ 10 (parse-long (subs (name node) 1)))))

(defn p2p-address
  [node info]
  (let [ip (node-ip node)]
    (or (first (filter #(and (str/includes? % (str "/ip4/" ip "/")) (str/includes? % "/p2p/"))
                       (:addresses info)))
        (if-let [id (known-peer-ids (name node))]
          (str "/ip4/" ip "/tcp/8228/p2p/" id)
          (throw (ex-info (str "node_info of " node " has no p2p address on " ip)
                          {:node node, :addresses (:addresses info)}))))))

(defn state-name
  "Normalises ChannelReady / CHANNEL_READY spellings to \"channelready\"."
  [channel]
  (-> channel :state :state_name str (str/replace "_" "") str/lower-case))

(defn ready-channel-count
  [node]
  (->> (:channels (rpc/call node "list_channels" {}))
       (filter #(= "channelready" (state-name %)))
       count))

(defn await-condition!
  [message timeout-ms pred]
  (await-fn (fn [] (or (pred) (throw (ex-info message {}))))
            {:log-message message, :timeout timeout-ms, :retry-interval 2000}))

(defn- graph-delta?
  "Whether `node`'s graph shows TLC expiry delta `delta` for both directions of
  every channel it knows."
  [node delta]
  (every? (fn [c] (every? #(= delta (some-> (get-in c [% :tlc_expiry_delta]) rpc/hex->int))
                          [:update_info_of_node1 :update_info_of_node2]))
          (:channels (rpc/call node "graph_channels" {}))))

(defn set-tlc-expiry-delta!
  "--tlc-expiry-delta: every node sets `delta` ms as the TLC expiry delta of each
  of its channels (update_channel), then the test waits until every node's graph
  shows it for both directions of every channel, so that routes use it. A
  release fnn refuses anything under 160 minutes (two thirds of its 4-hour
  epoch)."
  [test delta]
  (doseq [n  (:nodes test)
          ch (:channels (rpc/call n "list_channels" {}))]
    (try (rpc/call n "update_channel" {:channel_id       (:channel_id ch)
                                       :tlc_expiry_delta (rpc/int->hex delta)})
         (catch clojure.lang.ExceptionInfo e
           (throw (ex-info (str "--tlc-expiry-delta " delta ": " n " refused it (" (ex-message e)
                                "); a release build's minimum is 160 minutes (9600000)")
                           {:type :tlc-expiry-delta, :node n, :delta delta}
                           e)))))
  (doseq [n (:nodes test)]
    (await-condition! (str "Waiting for " n " to see TLC expiry delta " delta " ms on every channel")
                      180000
                      #(graph-delta? n delta))))

(defn setup!
  [test]
  (let [nodes (:nodes test)
        infos (into {} (map (fn [n] [n (rpc/call n "node_info")]) nodes))
        edges (edges test)]
    (info "Opening" (count edges) "channels for" (name (:topology test :line)) "over" nodes)
    (reset! (:pubkeys test) (update-vals infos :pubkey))
    (doseq [[a b] edges]
      (let [pubkey-b (:pubkey (infos b))]
        (info "Connecting" a "to" b)
        (rpc/call a "connect_peer" {:address (p2p-address b (infos b))})
        (await-condition! (str "Waiting for " a " to connect to " b) 30000
                          #(some (fn [p] (= pubkey-b (:pubkey p)))
                                 (:peers (rpc/call a "list_peers"))))
        (info "Opening a" funding-ckb "CKB channel" a "->" b)
        (rpc/call a "open_channel" {:pubkey         pubkey-b
                                    :funding_amount (rpc/int->hex (* funding-ckb shannons-per-ckb))
                                    :public         true})))
    (doseq [n nodes]
      (let [expected (count (filter #(some #{n} %) edges))]
        (await-condition! (str "Waiting for " expected " ready channels on " n) 180000
                          #(<= expected (ready-channel-count n)))))
    ; Every node must know every public channel before multi-hop routes exist.
    (doseq [n nodes]
      (await-condition! (str "Waiting for " n " to learn all channels via gossip") 180000
                        #(<= (count edges)
                             (count (:channels (rpc/call n "graph_channels" {}))))))
    (when-let [delta (:tlc-expiry-delta test)]
      (set-tlc-expiry-delta! test delta))
    (reset! (:initial-channels test)
            (into {} (for [n nodes]
                       [n (:channels (rpc/call n "list_channels" {}))])))
    (info "Topology ready:" edges)))
