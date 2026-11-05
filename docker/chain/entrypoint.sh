#!/usr/bin/env bash
# Runs the devnet node and, once its RPC answers, the miner. Exits if either dies.
set -euo pipefail
data=/fiber/tests/deploy/node-data

# Both run under libfaketime, with the offset in the chain-clock volume, which the
# control container shares: the outage fault (jepsen.fiber.faults.outage) writes
# a new offset there to move the chain's clock forward, and libfaketime rereads
# it once a second. Only the wall clock moves. Each start begins at +0.
clock=/chain-clock/faketime
mkdir -p "$(dirname "$clock")"
echo +0 >"$clock"
faketime=(env LD_PRELOAD=/usr/local/lib/libfaketimeMT.so.1
              FAKETIME_TIMESTAMP_FILE="$clock"
              FAKETIME_CACHE_DURATION=1
              FAKETIME_DONT_FAKE_MONOTONIC=1)

# CKB hashes the chain spec with the absolute paths of the contract files, so the
# snapshot (created elsewhere) fails the stored spec-hash check even though its
# genesis is identical. The genesis and contracts come from the same snapshot.
"${faketime[@]}" ckb run -C "$data" --indexer --skip-spec-check &
node_pid=$!

until curl -sf -H 'content-type: application/json' \
    -d '{"id":1,"jsonrpc":"2.0","method":"get_tip_block_number","params":[]}' \
    http://127.0.0.1:8114 >/dev/null; do
  sleep 0.5
done

"${faketime[@]}" ckb miner -C "$data" &
miner_pid=$!

wait -n "$node_pid" "$miner_pid"
