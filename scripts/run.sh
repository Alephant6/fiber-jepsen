#!/usr/bin/env bash
# Runs one Jepsen test from a fresh devnet. Arguments go to `lein run test`, e.g.
#   scripts/run.sh --time-limit 60 --faults kill
#   scripts/run.sh --time-limit 300 --faults kill,pause,partition --rate 10
# except --compose NAME, which adds docker/docker-compose.NAME.yml to the cluster
# (COMPOSE_FILE, if set, is the list it adds to), e.g. for cross-chain swaps:
#   scripts/run.sh --compose cch --scenario c-15
set -euo pipefail
cd "$(dirname "$0")/../docker"

args=()
while [ $# -gt 0 ]; do
  case "$1" in
    --compose)   COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}:docker-compose.$2.yml"; shift 2 ;;
    --compose=*) COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}:docker-compose.${1#--compose=}.yml"; shift ;;
    *)           args+=("$1"); shift ;;
  esac
done
[ -z "${COMPOSE_FILE:-}" ] || export COMPOSE_FILE

docker compose build
# Recreating the containers resets the chain to the snapshot and clears any
# leftover iptables rules or paused processes from an earlier run. Every
# service of the compose files starts.
docker compose up -d --force-recreate

docker compose exec -T control lein run test \
  --nodes n1,n2,n3 \
  --username root \
  --ssh-private-key /root/.ssh/id_rsa \
  "${args[@]}"
