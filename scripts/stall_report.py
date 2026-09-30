#!/usr/bin/env python3
"""Per-channel stall report for Jepsen runs, from the nodes' fnn.log and results.edn.

    scripts/stall_report.py store/fiber-kill/<run> [store/fiber-kill/<run> ...]

Needs channel debug logs (--log-level 'info,fnn::fiber::channel=debug') to see the nonce
ledger. For every channel with a problem, each end shows its final state and pending TLCs
(ends missing from the checker's channel-problems are ChannelReady with nothing pending),
whether a watchdog force-closed it, and whether it was blocked on an unbalanced revocation
nonce ledger at the end of the run. The channel is then

  stalled - some end is still ChannelReady and has been blocked on an unbalanced ledger
            (waiting_ack=false, send or verify nonce missing) for 10 s or more, up to the
            end of its log: the #1561 state;
  closed  - otherwise, some end is no longer ChannelReady;
  ok      - otherwise. It may still hold TLCs, e.g. ones waiting for a closed neighbour
            to settle on chain.

"FIRED" marks the error that the candidate #1561 watchdog logs when it force-closes a
channel (branch fix/1561-nonce-stall-watchdog on Alephant6/fiber).
"""
import datetime as dt
import pathlib
import re
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TIMESTAMP = re.compile(r"^\s*(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z")
LOG_CHANNEL = re.compile(r"channel: Hash256\(0x([0-9a-f]{8})")
BLOCKED = re.compile(r"retryable_ops_blocked waiting_ack=(\w+) send_present=(\w+) verify_present=(\w+)")
FIRED = re.compile(r"Channel (?:Hash256\()?0x([0-9a-f]{8})\S* .*revocation nonce ledger has been unbalanced")
PROBLEM = re.compile(
    r'\{:channel "0x([0-9a-f]{8})[0-9a-f]*", :node "(n\d)", :peer "n\d", :problems \[(.*?)\]\}'
)
STALL_SECONDS = 10


def node_log(path):
    """(channels the watchdog fired on, {channel: nonce state} for ends stalled at the end)."""
    fired, streaks, end = set(), {}, None
    for raw in path.open(errors="replace"):
        line = ANSI.sub("", raw)
        m = TIMESTAMP.match(line)
        if not m:
            continue
        t = dt.datetime.fromisoformat(m.group(1)[:26])
        end = t
        if f := FIRED.search(line):
            fired.add(f.group(1))
        b, c = BLOCKED.search(line), LOG_CHANNEL.search(line)
        if b and c:
            prev = streaks.get(c.group(1))
            # The retry loop logs a blocked line every 100 ms; a streak is a run of identical ones.
            if prev and prev[2] == b.groups() and (t - prev[1]).total_seconds() < 2:
                streaks[c.group(1)] = (prev[0], t, prev[2])
            else:
                streaks[c.group(1)] = (t, t, b.groups())
    stalled = {}
    for ch, (start, last, (waiting_ack, send, verify)) in streaks.items():
        if (waiting_ack == "false" and (end - last).total_seconds() <= 5
                and (last - start).total_seconds() >= STALL_SECONDS):
            stalled[ch] = f"send={'Y' if send == 'true' else 'N'}/verify={'Y' if verify == 'true' else 'N'}"
    return fired, stalled


def report(run_dir):
    text = " ".join((run_dir / "results.edn").read_text(errors="replace").split())
    problems = {}
    for ch, node, probs in PROBLEM.findall(text):
        state = re.search(r'\[:not-ready "(\w+)"\]', probs)
        tlcs = re.search(r"\[:pending-tlcs (\d+)\]", probs)
        problems[(ch, node)] = (state.group(1) if state else "Ready", int(tlcs.group(1)) if tlcs else 0)
    logs = {n: node_log(run_dir / n / "fnn.log") for n in ("n1", "n2", "n3")}
    channels = {ch for ch, _ in problems}
    for fired, stalled in logs.values():
        channels |= fired | set(stalled)
    lines = []
    for ch in sorted(channels):
        ends, verdict = [], "ok"
        for n, (fired, stalled) in logs.items():
            state, tlcs = problems.get((ch, n), ("Ready", 0))
            if (ch, n) not in problems and ch not in fired and ch not in stalled:
                continue
            if state != "Ready" and verdict == "ok":
                verdict = "closed"
            if state == "Ready" and ch in stalled:
                verdict = "stalled"
            ends.append(f"{n} {state}" + (f", {tlcs} TLCs" if tlcs else "") + (", FIRED" if ch in fired else "")
                        + (f", stalled {stalled[ch]}" if state == "Ready" and ch in stalled else ""))
        lines.append(f"  {ch} {verdict}: " + "; ".join(ends))
    return lines


def main():
    for arg in sys.argv[1:]:
        run_dir = pathlib.Path(arg)
        print(run_dir.name)
        print("\n".join(report(run_dir)) or "  no channel problems")


if __name__ == "__main__":
    main()
