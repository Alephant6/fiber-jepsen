#!/usr/bin/env bash
# Runs the devnet node and, once its RPC answers, the miner. Exits if either dies.
set -euo pipefail
data=/fiber/tests/deploy/node-data

# CKB hashes the chain spec with the absolute paths of the contract files, so the
# snapshot (created elsewhere) fails the stored spec-hash check even though its
# genesis is identical. The genesis and contracts come from the same snapshot.
ckb run -C "$data" --indexer --skip-spec-check &
node_pid=$!

until curl -sf -H 'content-type: application/json' \
    -d '{"id":1,"jsonrpc":"2.0","method":"get_tip_block_number","params":[]}' \
    http://127.0.0.1:8114 >/dev/null; do
  sleep 0.5
done

ckb miner -C "$data" &
miner_pid=$!

wait -n "$node_pid" "$miner_pid"
