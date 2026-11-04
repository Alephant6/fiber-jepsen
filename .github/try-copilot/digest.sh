#!/usr/bin/env bash
# Usage: digest.sh DIR  (DIR holds runs.txt, args-N.txt, run-N.log and store/)
# Prints a short Markdown digest of the failed runs for a model to read.
set -u -o pipefail
cd "$1"
strip() { sed -E 's/\x1b\[[0-9;]*m//g'; }
echo "# Runs"
while read -r i code store; do
  echo "- run $i: exit $code, $(cat "args-$i.txt" 2>/dev/null), store $store"
done <runs.txt
while read -r i code store; do
  [ "$code" -ne 0 ] || continue
  d="store/$store"
  echo
  echo "# Failed run $i ($store)"
  echo "## results.edn (hashes shortened, first 3000 bytes)"
  echo '```'
  sed -E 's/0x[0-9a-f]{16,}/0x…/g' "$d/results.edn" 2>/dev/null | tr -s " " | head -c 3000
  echo
  echo '```'
  for f in "$d"/n*/fnn.log; do
    [ -f "$f" ] || continue
    n="$(basename "$(dirname "$f")")"
    echo "## $n fnn.log: WARN/ERROR lines, timestamps dropped, counted (top 12)"
    echo '```'
    strip <"$f" | grep -E '^ *[0-9T:.-]+Z +(WARN|ERROR) ' \
      | sed -E 's/^ *[0-9T:.-]+Z +//; s/0x[0-9a-f]{16,}/0x…/g; s/Pubkey\([0-9a-f]+\)/Pubkey(…)/g' \
      | cut -c1-220 | sort | uniq -c | sort -rn | head -n 12
    echo '```'
  done
done <runs.txt
