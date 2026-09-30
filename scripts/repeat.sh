#!/usr/bin/env bash
# Runs the same test several times and prints one summary line per run.
#   scripts/repeat.sh 3 --time-limit 60 --faults kill --kill-targets one
set -uo pipefail
here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
runs="$1"; shift
for i in $(seq 1 "$runs"); do
  log="$(mktemp)"
  bash "$here/run.sh" "$@" >"$log" 2>&1
  verdict="$(grep -oE 'Everything looks good|Analysis invalid|Errors occurred during analysis|Jepsen broke' "$log" | tail -1)"
  stuck="$(grep -c ':status "Inflight"\|:status "Created"' "$log")"
  tlcs="$(grep -c ':pending-tlcs' "$log")"
  if grep -q ':probe-failures \[\]' "$log"; then probe="ok"; else probe="FAILED"; fi
  echo "run $i/$runs: ${verdict:-no verdict}; unfinished payments: $stuck;" \
       "channel ends with pending TLCs: $tlcs; post-heal probe: $probe"
  rm -f "$log"
done
