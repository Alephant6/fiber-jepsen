#!/usr/bin/env bash
# Runs the devnet node and, once its RPC answers, the miner. Exits if either dies.
set -euo pipefail
data=/fiber/tests/deploy/node-data

# Both run with clockshift.c preloaded, which adds the offset in the chain-clock
# volume to the wall clock: the outage fault (jepsen.fiber.faults.outage) writes
# a new offset there from the control container to move the chain's clock
# forward, and the wrapper rereads it once a second. Monotonic clocks don't
# move. Each start begins at +0. (Not libfaketime, as on the fnn nodes: ckb links
# jemalloc, and a jemalloc program hangs at start under libfaketime 0.9.10.)
clock=/chain-clock/offset
mkdir -p "$(dirname "$clock")"
echo +0 >"$clock"
shifted=(env LD_PRELOAD=/usr/local/lib/libclockshift.so CLOCKSHIFT_FILE="$clock")

# CKB hashes the chain spec with the absolute paths of the contract files, so the
# snapshot (created elsewhere) fails the stored spec-hash check even though its
# genesis is identical. The genesis and contracts come from the same snapshot.
"${shifted[@]}" ckb run -C "$data" --indexer --skip-spec-check &
node_pid=$!

until curl -sf -H 'content-type: application/json' \
    -d '{"id":1,"jsonrpc":"2.0","method":"get_tip_block_number","params":[]}' \
    http://127.0.0.1:8114 >/dev/null; do
  sleep 0.5
done

"${shifted[@]}" ckb miner -C "$data" &
miner_pid=$!

wait -n "$node_pid" "$miner_pid"
