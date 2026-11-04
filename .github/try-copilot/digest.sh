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
    echo "## $n fnn.log: process starts (Jepsen restarts) $(grep -c 'Jepsen starting' "$f")"
    # Close, force-close and panic lines are rare, so the top-12 count below can
    # drop them; keep every distinct one, with the time it first appeared.
    echo "## $n fnn.log: close / shutdown / panic lines, first time seen and count (always kept)"
    echo '```'
    strip <"$f" | grep -E '^ *[0-9T:.-]+Z +[A-Z]+ ' \
      | grep -iE 'forcefully|force.?clos|closing transaction|shutting ?down|shutdown channel|ChannelStopped|abort|panicked' \
      | sed -E 's/^ *([0-9T:.-]+)Z +/\1 /; s/0x[0-9a-f]{16,}/0x…/g; s/Pubkey\([0-9a-f]+\)/Pubkey(…)/g' \
      | cut -c1-220 \
      | awk '{t=$1; $1=""; k=substr($0,2); if (!(k in c)) {o[++m]=k; f[k]=t} c[k]++} END {for (i=1;i<=m && i<=20;i++) print f[o[i]] " x" c[o[i]] " " o[i]}'
    echo '```'
    echo "## $n fnn.log: WARN/ERROR lines, timestamps dropped, counted (top 12)"
    echo '```'
    strip <"$f" | grep -E '^ *[0-9T:.-]+Z +(WARN|ERROR) ' \
      | sed -E 's/^ *[0-9T:.-]+Z +//; s/0x[0-9a-f]{16,}/0x…/g; s/Pubkey\([0-9a-f]+\)/Pubkey(…)/g' \
      | cut -c1-220 | sort | uniq -c | sort -rn | head -n 12
    echo '```'
  done
done <runs.txt
