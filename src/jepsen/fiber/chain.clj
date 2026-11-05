(ns jepsen.fiber.chain
  "Reads the CKB devnet (the chain container, which runs ckb with the indexer):
  transactions, the transactions that touch a lock script, and the capacity a
  lock script holds. Lock scripts are maps as the RPC returns them
  ({:code_hash :hash_type :args})."
  (:require [jepsen.fiber.rpc :as rpc]))

(def url
  "The CKB node's RPC, as the control node reaches it (docker/docker-compose.yml)."
  "http://chain:8114")

(def page-size
  "Indexer results per call."
  200)

(defn call
  [method & params]
  (rpc/call-url url method params {:timeout 15000}))

(defn lock-key
  "A string that identifies lock script `script`, for comparing and as a map key."
  [script]
  (str (:code_hash script) "/" (name (:hash_type script)) "/" (:args script)))

(defn tip
  "The tip block number."
  []
  (long (rpc/hex->int (call "get_tip_block_number"))))

(defn tip-time-ms
  "The tip block's timestamp, in ms since the epoch: the CKB node's clock when it
  made the block's template (the miner keeps it)."
  []
  (long (rpc/hex->int (:timestamp (call "get_tip_header")))))

(defn transaction
  "get_transaction's result for `hash`: {:transaction {...} :tx_status {...}}, or nil."
  [hash]
  (call "get_transaction" hash))

(defn- search-key
  [script]
  {:script script, :script_type "lock", :script_search_mode "exact"})

(defn transactions-of
  "Every committed transaction input or output locked by `script`, in block
  order: [{:tx_hash :block_number :io_type \"input\"|\"output\" :io_index} ...]."
  [script]
  (loop [after nil, acc []]
    (let [res  (if after
                 (call "get_transactions" (search-key script) "asc" (rpc/int->hex page-size) after)
                 (call "get_transactions" (search-key script) "asc" (rpc/int->hex page-size)))
          page (:objects res)
          acc  (into acc page)]
      (if (and (= page-size (count page)) (:last_cursor res))
        (recur (:last_cursor res) acc)
        acc))))

(defn capacity-of
  "{:capacity shannons, :block n}: the total capacity of the live cells locked by
  `script`, and the block the indexer had reached."
  [script]
  (if-let [res (call "get_cells_capacity" (search-key script))]
    {:capacity (long (rpc/hex->int (:capacity res)))
     :block    (long (rpc/hex->int (:block_number res)))}
    {:capacity 0, :block (tip)}))
