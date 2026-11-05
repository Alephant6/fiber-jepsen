#!/usr/bin/env bash
# Builds everything the Docker images need into docker/build/:
#   - bin/fnn, bin/ckb           the binaries under test
#   - chain/tests/deploy/...     an initialised CKB devnet (funded wallets, contracts, UDT)
#   - nodes/{1,...,6}/           Fiber node directories with configs patched for the Docker network
#   - ssh/id_rsa{,.pub}          the key the Jepsen control node uses to reach the fnn nodes
#
# The devnet is created with Fiber's own scripts (tests/deploy/init-dev-chain.sh), so every
# test run starts from exactly the state Fiber's e2e tests use. Fiber's tests/nodes has
# node directories for 1-3 only; nodes 4-6, for multi-node topologies, get keys made here
# from their numbers (the same in every snapshot) and CKB from the devnet's deployer
# account, but no UDT. The extra transactions are the only change to the chain the
# three-node tests see.
#
# Environment:
#   FIBER_SNAP  a Fiber checkout used only for generating the snapshot (default ~/fiber-snap)
#   FNN_BIN     the fnn binary to test (default ~/fiber/target/release/fnn). Set it empty
#               to leave bin/fnn out: nothing else depends on fnn, so CI caches the rest
#               by Fiber's test files and adds the fnn of each commit afterwards.
# Requires ckb and ckb-cli in PATH (Fiber's CI uses ckb 0.202.0), nc, and python3 with PyYAML.
set -euo pipefail

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$here")"
out="$root/docker/build"
FIBER_SNAP="${FIBER_SNAP:-$HOME/fiber-snap}"
FNN_BIN="${FNN_BIN-$HOME/fiber/target/release/fnn}"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

for cmd in ckb ckb-cli nc python3 ssh-keygen; do
  command -v "$cmd" >/dev/null || { echo "missing $cmd" >&2; exit 1; }
done
[ -z "$FNN_BIN" ] || [ -x "$FNN_BIN" ] || { echo "missing fnn binary at $FNN_BIN" >&2; exit 1; }

echo "== Initialising the devnet in $FIBER_SNAP"
cd "$FIBER_SNAP"
rm -rf tests/nodes/*/fiber/store
./tests/deploy/init-dev-chain.sh -f
# init-dev-chain.sh stops ckb with SIGTERM and returns immediately; copying the
# data directory before ckb has finished flushing could capture a torn database.
while pgrep -x ckb >/dev/null; do sleep 0.5; done

# Nodes beyond Fiber's three: n4-n6 in docker/docker-compose.yml.
extra_nodes=(4 5 6)
data_dir="$FIBER_SNAP/tests/deploy/node-data"
keys="$(mktemp -d)"
trap 'rm -rf "$keys"' EXIT
ckb_cli() {
  # As init-dev-chain.sh calls it: don't pollute the home directory.
  env HOME="$data_dir" ckb-cli --url http://127.0.0.1:8114 "$@"
}
# The full address of a key file (wallet files in Fiber's tests/nodes use this format).
address_of() {
  ckb_cli util key-info --privkey-path "$1" 2>&1 \
    | grep -oE 'ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsq[0-9a-z]+' | head -n 1 || true
}
# A devnet-only key: sha256 of a label naming the node. Never use these keys elsewhere.
derive_hex() {
  python3 -c 'import hashlib, sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' "$1"
}
derive_raw() {
  python3 -c 'import hashlib, sys; sys.stdout.buffer.write(hashlib.sha256(sys.argv[1].encode()).digest())' "$1"
}

echo "== Funding nodes ${extra_nodes[*]} on the devnet"
ckb run -C "$data_dir" --indexer >"$keys/ckb.log" 2>&1 &
ckb_pid=$!
for _ in $(seq 1 60); do nc -z 127.0.0.1 8114 && break; sleep 1; done
nc -z 127.0.0.1 8114 || { echo "ckb didn't start; its log:" >&2; cat "$keys/ckb.log" >&2; exit 1; }
# Check the address extraction against a node whose wallet Fiber records.
[ "$(address_of tests/nodes/1/ckb/plain_key)" = "$(tr -d '[:space:]' <tests/nodes/1/ckb/wallet)" ] \
  || { echo "ckb-cli util key-info doesn't give node 1's wallet address" >&2; exit 1; }
for i in "${extra_nodes[@]}"; do
  mkdir -p "$keys/$i/ckb" "$keys/$i/fiber"
  # fnn reads a plain hex key and encrypts it in place with FIBER_SECRET_KEY_PASSWORD.
  derive_hex "fiber-jepsen devnet node $i ckb key" >"$keys/$i/ckb/key"
  derive_raw "fiber-jepsen devnet node $i fiber key" >"$keys/$i/fiber/sk"
  chmod 600 "$keys/$i/ckb/key" "$keys/$i/fiber/sk"
  address_of "$keys/$i/ckb/key" >"$keys/$i/ckb/wallet"
  [ -s "$keys/$i/ckb/wallet" ] || { echo "no address for node $i" >&2; exit 1; }
  # Five cells, as init-dev-chain.sh gives nodes 1-3, so that a node can fund
  # several channels at once.
  for _ in 1 2 3 4 5; do
    ckb_cli wallet transfer --to-address "$(cat "$keys/$i/ckb/wallet")" --capacity 100000 \
      --fee-rate 2000 --privkey-path tests/nodes/deployer/ckb/plain_key
    sleep 1
    ./tests/deploy/generate-blocks.sh 4 >/dev/null
    sleep 1
  done
done
./tests/deploy/generate-blocks.sh 4 >/dev/null
for i in "${extra_nodes[@]}"; do
  # The indexer may be a block or two behind.
  for _ in $(seq 1 10); do
    balance="$(ckb_cli wallet get-capacity --address "$(cat "$keys/$i/ckb/wallet")" 2>&1 || true)"
    total="$(grep -oE 'total: *[0-9.]+' <<<"$balance" | grep -oE '[0-9.]+$' || true)"
    [ "${total%%.*}" = 500000 ] && break
    sleep 1
  done
  echo "node $i: $(cat "$keys/$i/ckb/wallet"): ${total:-?} CKB"
  [ "${total%%.*}" = 500000 ] \
    || { echo "node $i wasn't funded with 500000 CKB: $balance" >&2; exit 1; }
done
kill "$ckb_pid"
wait "$ckb_pid" || true
while pgrep -x ckb >/dev/null; do sleep 0.5; done

echo "== Staging build artefacts in $out"
rm -rf "$out"
mkdir -p "$out"/{bin,chain/tests/deploy,nodes,ssh}
[ -z "$FNN_BIN" ] || cp "$FNN_BIN" "$out/bin/fnn"
cp "$(command -v ckb)" "$out/bin/ckb"

# Keep the tests/deploy layout: specs/dev.toml refers to the contracts by relative path.
cp -r tests/deploy/node-data tests/deploy/contracts "$out/chain/tests/deploy/"
ckb_toml="$out/chain/tests/deploy/node-data/ckb.toml"
miner_toml="$out/chain/tests/deploy/node-data/ckb-miner.toml"
# Let the fnn containers reach the CKB RPC, and mine one block per second.
sed -i 's|^listen_address = "127.0.0.1:8114"|listen_address = "0.0.0.0:8114"|' "$ckb_toml"
grep -q '^listen_address = "0.0.0.0:8114"' "$ckb_toml"
sed -i -E 's|^(value = )[0-9]+|\11000|' "$miner_toml"

for i in 1 2 3; do
  mkdir -p "$out/nodes/$i"
  cp -r "tests/nodes/$i/fiber" "tests/nodes/$i/ckb" "$out/nodes/$i/"
  rm -rf "$out/nodes/$i/fiber/store"
  cp "tests/nodes/$i/dev.toml" "$out/nodes/$i/dev.toml"
  python3 "$here/patch_node_config.py" "tests/nodes/$i/config.yml" "$out/nodes/$i/config.yml" "10.77.0.1$i"
done
# udt-init writes the same config for every node but its addresses and name, so the
# extra nodes start from node 1's.
for i in "${extra_nodes[@]}"; do
  mkdir -p "$out/nodes/$i"
  cp -r "$keys/$i/ckb" "$keys/$i/fiber" "$out/nodes/$i/"
  cp tests/nodes/1/dev.toml "$out/nodes/$i/dev.toml"
  python3 "$here/patch_node_config.py" tests/nodes/1/config.yml "$out/nodes/$i/config.yml" \
    "10.77.0.$((10 + i))" "fiber-$i"
done

ssh-keygen -q -t rsa -b 4096 -N '' -C fiber-jepsen -f "$out/ssh/id_rsa"

echo "== Done"
du -sh "$out"/*
