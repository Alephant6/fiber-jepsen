#!/usr/bin/env bash
# Runs one Jepsen test from a fresh devnet. Arguments go to `lein run test`, e.g.
#   scripts/run.sh --time-limit 60 --faults kill
#   scripts/run.sh --time-limit 300 --faults kill,pause,partition --rate 10
#   scripts/run.sh --nodes n1,n2,n3,n4,n5,n6 --topology mesh --time-limit 60 --faults kill
# The test runs on n1-n3 unless the arguments name other nodes with --nodes
# (the last --nodes wins, so a scenario's or the caller's replaces the default).
# Except --compose NAME, which adds docker/docker-compose.NAME.yml and its
# services to the cluster (COMPOSE_FILE, if set, is the list it adds to), e.g.
# for cross-chain swaps, for power loss, which needs LazyFS, or for slow IO,
# which needs unreliablefs:
#   scripts/run.sh --compose cch --scenario c-15
#   scripts/run.sh --compose lazyfs --scenario power-sync-on
#   scripts/run.sh --compose slowio --scenario slow-io
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
# --faults power puts fnn's store on LazyFS, and --faults slow-io on
# unreliablefs. Each needs its filesystem in the node image and FUSE in the node
# containers (docker-compose.lazyfs.yml, docker-compose.slowio.yml). A
# scenario's faults aren't visible here, so such a scenario passes --compose
# lazyfs or --compose slowio (the matrix does: scenarios.clj's :compose).
has_fault() {
  local want="$1" prev="" a list
  for a in "${args[@]}"; do
    list=""
    if [ "$prev" = --faults ]; then list="$a"; fi
    case "$a" in --faults=*) list="${a#--faults=}" ;; esac
    if [[ ",$list," == *,"$want",* ]]; then return 0; fi
    prev="$a"
  done
  return 1
}
lazyfs=0
slowio=0
has_fault power && lazyfs=1
has_fault slow-io && slowio=1
rest=()
for name in "${extra[@]}"; do
  case "$name" in
    lazyfs) lazyfs=1 ;;
    slowio) slowio=1 ;;
    *)      rest+=("$name") ;;
  esac
done
extra=(${rest[@]+"${rest[@]}"})
if [ "$lazyfs" = 1 ]; then
  base="$base:docker-compose.lazyfs.yml"
fi
if [ "$slowio" = 1 ]; then
  base="$base:docker-compose.slowio.yml"
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
