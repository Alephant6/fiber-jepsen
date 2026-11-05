#!/usr/bin/env bash
# Runs one Jepsen test from a fresh devnet. Arguments go to `lein run test`, e.g.
#   scripts/run.sh --time-limit 60 --faults kill
#   scripts/run.sh --time-limit 300 --faults kill,pause,partition --rate 10
#   scripts/run.sh --nodes n1,n2,n3,n4,n5,n6 --topology mesh --time-limit 60 --faults kill
# The test runs on n1-n3 unless the arguments name other nodes with --nodes
# (the last --nodes wins, so a scenario's or the caller's replaces the default).
# Except --compose NAME, which adds docker/docker-compose.NAME.yml and its
# services to the cluster (COMPOSE_FILE, if set, is the list it adds to), e.g.
# for cross-chain swaps, or for power loss, which needs LazyFS:
#   scripts/run.sh --compose cch --scenario c-15
#   scripts/run.sh --compose lazyfs --scenario power-sync-on
set -euo pipefail
cd "$(dirname "$0")/../docker"

args=()
extra=()
while [ $# -gt 0 ]; do
  case "$1" in
    --compose)   extra+=("$2"); shift 2 ;;
    --compose=*) extra+=("${1#--compose=}"); shift ;;
    *)           args+=("$1"); shift ;;
  esac
done
base="${COMPOSE_FILE:-docker-compose.yml}"
# --faults power puts fnn's store on LazyFS, which needs it in the node image and
# FUSE in the node containers (docker-compose.lazyfs.yml). A scenario's faults
# aren't visible here, so a power scenario passes --compose lazyfs (the matrix
# does: scenarios.clj's :compose).
power_fault() {
  local prev="" a list
  for a in "${args[@]}"; do
    list=""
    if [ "$prev" = --faults ]; then list="$a"; fi
    case "$a" in --faults=*) list="${a#--faults=}" ;; esac
    if [[ ",$list," == *,power,* ]]; then return 0; fi
    prev="$a"
  done
  return 1
}
lazyfs=0
power_fault && lazyfs=1
rest=()
for name in "${extra[@]}"; do
  if [ "$name" = lazyfs ]; then lazyfs=1; else rest+=("$name"); fi
done
extra=(${rest[@]+"${rest[@]}"})
if [ "$lazyfs" = 1 ]; then
  base="$base:docker-compose.lazyfs.yml"
fi
COMPOSE_FILE="$base"
for name in ${extra[@]+"${extra[@]}"}; do COMPOSE_FILE="$COMPOSE_FILE:docker-compose.$name.yml"; done
export COMPOSE_FILE
# The services the --compose files add, which start with the cluster.
added=()
if [ ${#extra[@]} -gt 0 ]; then
  mapfile -t added < <(comm -13 <(COMPOSE_FILE="$base" docker compose config --services | sort) \
                                <(docker compose config --services | sort))
fi

docker compose build
# Recreating the containers resets the chain to the snapshot and clears any
# leftover iptables rules or paused processes from an earlier run. Every node
# container starts; the ones the test doesn't use only run sshd.
mapfile -t nodes < <(docker compose config --services | grep -E '^n[0-9]+$')
docker compose up -d --force-recreate chain "${nodes[@]}" control "${added[@]}"

docker compose exec -T control lein run test \
  --nodes n1,n2,n3 \
  --username root \
  --ssh-private-key /root/.ssh/id_rsa \
  "${args[@]}"
