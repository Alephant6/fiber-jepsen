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
  stuck="$(grep -oE ':status "(Inflight|Created)"' "$log" | wc -l)"
  tlcs="$(grep -o ':pending-tlcs' "$log" | wc -l)"
  if grep -q ':probe-failures \[\]' "$log"; then probe="ok"; else probe="FAILED"; fi
  # pprint may put the map on the line after its key.
  summary="$(tr '\n' ' ' <"$log" | grep -oE ':channel-summary +\{[^}]*\}' | tail -1 | tr -s ' ')"
  # Only with --nonce-check: nonces whose partial signatures left a node in two contexts.
  nonce="$(tr '\n' ' ' <"$log" | grep -oE ':nonce-violations +[0-9]+' | tail -1 | grep -oE '[0-9]+$')"
  store="$(readlink "$here/../store/latest" 2>/dev/null)"
  echo "run $i/$runs: ${verdict:-no verdict}; unfinished payments: $stuck;" \
       "channel ends with pending TLCs: $tlcs; post-heal probe: $probe; ${summary:-no summary};" \
       "nonce violations: ${nonce:--}; store: ${store:-unknown}"
  rm -f "$log"
done
