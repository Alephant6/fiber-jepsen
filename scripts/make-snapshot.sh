#!/usr/bin/env bash
# Builds everything the Docker images need into docker/build/:
#   - bin/fnn, bin/ckb           the binaries under test
#   - chain/tests/deploy/...     an initialised CKB devnet (funded wallets, contracts, UDT)
#   - nodes/{1,2,3}/             Fiber node directories with configs patched for the Docker network
#   - ssh/id_rsa{,.pub}          the key the Jepsen control node uses to reach the fnn nodes
#
# The devnet is created with Fiber's own scripts (tests/deploy/init-dev-chain.sh), so every
# test run starts from exactly the state Fiber's e2e tests use.
#
# Environment:
#   FIBER_SNAP  a Fiber checkout used only for generating the snapshot (default ~/fiber-snap)
#   FNN_BIN     the fnn binary to test (default ~/fiber/target/release/fnn)
# Requires ckb and ckb-cli in PATH (Fiber's CI uses ckb 0.202.0), nc, and python3 with PyYAML.
set -euo pipefail

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$here")"
out="$root/docker/build"
FIBER_SNAP="${FIBER_SNAP:-$HOME/fiber-snap}"
FNN_BIN="${FNN_BIN:-$HOME/fiber/target/release/fnn}"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

for cmd in ckb ckb-cli nc python3 ssh-keygen; do
  command -v "$cmd" >/dev/null || { echo "missing $cmd" >&2; exit 1; }
done
[ -x "$FNN_BIN" ] || { echo "missing fnn binary at $FNN_BIN" >&2; exit 1; }

echo "== Initialising the devnet in $FIBER_SNAP"
cd "$FIBER_SNAP"
rm -rf tests/nodes/*/fiber/store
./tests/deploy/init-dev-chain.sh -f
# init-dev-chain.sh stops ckb with SIGTERM and returns immediately; copying the
# data directory before ckb has finished flushing could capture a torn database.
while pgrep -x ckb >/dev/null; do sleep 0.5; done

echo "== Staging build artefacts in $out"
rm -rf "$out"
mkdir -p "$out"/{bin,chain/tests/deploy,nodes,ssh}
cp "$FNN_BIN" "$out/bin/fnn"
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

ssh-keygen -q -t rsa -b 4096 -N '' -C fiber-jepsen -f "$out/ssh/id_rsa"

echo "== Done"
du -sh "$out"/*
