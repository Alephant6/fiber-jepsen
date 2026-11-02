#!/usr/bin/env bash
# Builds fnn with the MuSig2 nonce-use oracle (instrument/), for the nonce check.
# The Fiber checkout is modified in place, so use a clean worktree of the commit under test.
#   scripts/build-fnn-nonce-oracle.sh /path/to/fiber                       # measured build
#   scripts/build-fnn-nonce-oracle.sh /path/to/fiber nonce-oracle-plant    # planted-reuse self-test
# Then copy the printed binary to docker/build/bin/fnn.
set -euo pipefail
here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
fiber="$1"
feature="${2:-nonce-oracle}"
if [ ! -f "$fiber/crates/fiber-lib/src/fiber/nonce_oracle.rs" ]; then
  python3 "$here/../instrument/apply_nonce_oracle.py" "$fiber"
fi
cd "$fiber"
cargo build --release --locked -p fiber-bin --features "fnn/$feature"
echo "built ${CARGO_TARGET_DIR:-$fiber/target}/release/fnn with fnn/$feature"
