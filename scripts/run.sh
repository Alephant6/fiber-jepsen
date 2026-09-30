#!/usr/bin/env bash
# Runs one Jepsen test from a fresh devnet. Arguments go to `lein run test`, e.g.
#   scripts/run.sh --time-limit 60 --faults kill
#   scripts/run.sh --time-limit 300 --faults kill,pause,partition --rate 10
set -euo pipefail
cd "$(dirname "$0")/../docker"

docker compose build
# Recreating the containers resets the chain to the snapshot and clears any
# leftover iptables rules or paused processes from an earlier run.
docker compose up -d --force-recreate chain n1 n2 n3 control

docker compose exec -T control lein run test \
  --nodes n1,n2,n3 \
  --username root \
  --ssh-private-key /root/.ssh/id_rsa \
  "$@"
