#!/usr/bin/env bash
# Runs one Jepsen test from a fresh devnet. Arguments go to `lein run test`, e.g.
#   scripts/run.sh --time-limit 60 --faults kill
#   scripts/run.sh --time-limit 300 --faults kill,pause,partition --rate 10
#   scripts/run.sh --nodes n1,n2,n3,n4,n5,n6 --topology mesh --time-limit 60 --faults kill
# The test runs on n1-n3 unless the arguments name other nodes with --nodes
# (the last --nodes wins, so a scenario's or the caller's replaces the default).
set -euo pipefail
cd "$(dirname "$0")/../docker"

# --faults power puts fnn's store on LazyFS, which needs it in the node image and
# FUSE in the node containers (docker-compose.lazyfs.yml).
if [[ "$*" == *power* ]]; then
  export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}:docker-compose.lazyfs.yml"
fi

docker compose build
# Recreating the containers resets the chain to the snapshot and clears any
# leftover iptables rules or paused processes from an earlier run. Every node
# container starts; the ones the test doesn't use only run sshd.
mapfile -t nodes < <(docker compose config --services | grep -E '^n[0-9]+$')
docker compose up -d --force-recreate chain "${nodes[@]}" control

docker compose exec -T control lein run test \
  --nodes n1,n2,n3 \
  --username root \
  --ssh-private-key /root/.ssh/id_rsa \
  "$@"
