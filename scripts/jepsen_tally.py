#!/usr/bin/env python3
"""Tallies Jepsen runs listed in a repeat.sh log: verdict, channel classes, completions, bad signatures.
   jepsen_tally.py <repeat-log> <store-root>
"""
import re, sys, subprocess, pathlib, collections
log, store = sys.argv[1], pathlib.Path(sys.argv[2])
ANSI = re.compile(r"\x1b\[[0-9;]*m")
label = None
batches = collections.OrderedDict()
for line in open(log, errors="replace"):
    m = re.match(r"== (.*?) \(\d\d:\d\d UTC\)", line)
    if m and not line.startswith(("== uniform:", "== mixed:")):
        label = m.group(1)
        batches[label] = []
        continue
    m = re.match(r"run (\d+)/(\d+): (.*?); unfinished payments: (\d+);.*?post-heal probe: (\w+);.*?store: (\S+)", line)
    if m and label:
        batches[label].append((m.group(1), m.group(6), m.group(3)))
here = pathlib.Path(__file__).parent
print("%-34s %4s %6s %8s %8s %6s %7s %8s %8s" % ("batch", "runs", "passed", "stalled", "closed", "leftTLC", "reply+", "reest+", "badsig"))
for label, runs in batches.items():
    n = len(runs)
    passed = stalled = closed = left = reply = reest = bad = 0
    for _, d, verdict in runs:
        rd = store / d
        if "Everything looks good" in verdict:
            passed += 1
        rep = subprocess.run([sys.executable, str(here / "stall_report.py"), str(rd)], capture_output=True, text=True).stdout
        if re.search(r"\bstalled:", rep): stalled += 1
        if re.search(r"\bclosed:", rep): closed += 1
        if re.search(r"\bok: .*TLCs", rep): left += 1
        for node in ("n1", "n2", "n3"):
            p = rd / node / "fnn.log"
            if not p.exists():
                continue
            text = ANSI.sub("", p.read_text(errors="replace"))
            reply += text.count("[ack] rebalance: CommitmentSigned")
            reest += text.count("[ack] rebalance on reestablish")
            bad += text.count("Musig2VerifyError(BadSignature)") + text.count("is reused for different messages")
    print("%-34s %4d %6d %8d %8d %6d %7d %8d %8d" % (label, n, passed, stalled, closed, left, reply, reest, bad))
