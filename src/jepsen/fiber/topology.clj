(ns jepsen.fiber.topology
  "Builds the channel graph before the workload starts: a line n1 - n2 - n3 of
  public channels, so payments between the ends are forwarded by the middle
  node. Records each node's starting channels for the conservation check."
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
  node_info doesn't report a full multiaddr."
  {"n1" "QmbvRjJHAQDmj3cgnUBGQ5zVnGxUKwb2qJygwNs2wk41h8"
   "n2" "QmSRcPqUn4aQrKHXyCDjGn2qBVf43tWBDS2Wj9QDUZXtZp"
   "n3" "QmaFDJb9CkMrXy7nhTWBY5y9mvuykre3EzzRsCJUAVXprZ"})

(defn edges
  "Channels to open, as [opener acceptor] pairs: a line through all nodes."
  [nodes]
  (map vec (partition 2 1 nodes)))

(defn node-ip
  [node]
  (str "10.77.0.1" (subs (name node) 1)))

(defn p2p-address
  [node info]
  (or (first (filter #(and (str/includes? % (node-ip node)) (str/includes? % "/p2p/"))
                     (:addresses info)))
      (str "/ip4/" (node-ip node) "/tcp/8228/p2p/" (known-peer-ids (name node)))))

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

(defn setup!
  [test]
  (let [nodes (:nodes test)
        infos (into {} (map (fn [n] [n (rpc/call n "node_info")]) nodes))
        edges (edges nodes)]
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
    (reset! (:initial-channels test)
            (into {} (for [n nodes]
                       [n (:channels (rpc/call n "list_channels" {}))])))
    (info "Topology ready:" edges)))
