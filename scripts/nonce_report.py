#!/usr/bin/env python3
"""MuSig2 nonce-use report for Jepsen runs, from each node's nonce-oracle.log.

    scripts/nonce_report.py store/fiber-kill/<run> [store/fiber-kill/<run> ...]
    scripts/nonce_report.py --file some/nonce-oracle.log      # e.g. a unit-test run

Same rules as the Jepsen checker (src/jepsen/fiber/nonce.clj). A node's file is pooled
across its restarts. A nonce whose partial signature left the node in two different
contexts is a violation. The other columns say how much of the recovery path a clean run
exercised:

  procs      processes that wrote records (1 + restarts that signed something)
  sigs       signatures recorded (Sent + Aggregated)
  viol       nonces sent in two or more contexts
  cached     V2 replays of a stored signature (same nonce, same context)
  c-restart  of those, replays by a later process than the one that signed
  resigned   nonces whose same context was signed again by a later process
  conflict   V2 refusals to bind a consumed nonce to a different context
  reest      channel reestablishments started (V1 + V2)
  rep-cs     V2 reestablishments that resent a stored CommitmentSigned verbatim
  rep-ack    V2 reestablishments that resent a cached RevokeAndAck verbatim
  1+agg      nonces sent once that also aggregated a different context (V1 design)
  agg2+      nonces that aggregated two or more contexts (V1)
"""
import collections
import pathlib
import sys

COLUMNS = ["procs", "sigs", "viol", "cached", "c-restart", "resigned", "conflict", "reest", "rep-cs", "rep-ack",
           "1+agg", "agg2+"]


def parse(path):
    records = []
    for line in open(path, errors="replace"):
        if not line.startswith("NONCE-ORACLE "):
            continue
        parts = line.split()
        r = {"kind": parts[1]}
        for field in parts[2:]:
            k, sep, v = field.partition("=")
            if sep:
                r[k] = v
        r["proc"] = f"{r.get('pid')}/{r.get('boot')}"
        records.append(r)
    return records


def analyze(records):
    sent = collections.defaultdict(lambda: collections.defaultdict(set))  # nonce -> ctx -> procs
    agg = collections.defaultdict(set)  # nonce -> ctxs
    sites = collections.defaultdict(set)  # (nonce, ctx) -> sites
    first_proc = {}
    cached = []
    out = collections.Counter()
    procs = set()
    for r in records:
        procs.add(r["proc"])
        kind, nonce, ctx = r["kind"], r.get("nonce"), r.get("context")
        if kind == "Sent":
            sent[nonce][ctx].add(r["proc"])
            sites[(nonce, ctx)].add(r.get("site", "?"))
            first_proc.setdefault(nonce, r["proc"])
        elif kind == "Aggregated":
            agg[nonce].add(ctx)
        elif kind == "Cached":
            cached.append(r)
        elif kind == "Conflict":
            out["conflict"] += 1
        elif kind == "Reestablish":
            out["reest"] += 1
        elif kind == "Replay":
            out["rep-cs"] += r.get("cs") == "true"
            out["rep-ack"] += r.get("ack") == "true"
    nonces = set(sent) | set(agg)
    violations = []
    for n in nonces:
        s = sent.get(n, {})
        if len(s) >= 2:
            violations.append((n, {c: (sorted(p), sorted(sites[(n, c)])) for c, p in s.items()}))
        if len(s) == 1 and any(c not in s for c in agg.get(n, ())):
            out["1+agg"] += 1
        if len(agg.get(n, ())) >= 2:
            out["agg2+"] += 1
        if any(len(p) > 1 for p in s.values()):
            out["resigned"] += 1
    out["procs"] = len(procs)
    out["sigs"] = sum(1 for r in records if r["kind"] in ("Sent", "Aggregated"))
    out["viol"] = len(violations)
    out["cached"] = len(cached)
    out["c-restart"] = sum(1 for r in cached if first_proc.get(r["nonce"]) not in (None, r["proc"]))
    return out, violations


def row(label, out):
    return f"{label:44} " + " ".join(f"{out[c]:>9}" for c in COLUMNS)


def main():
    args = sys.argv[1:]
    print(f"{'run / node':44} " + " ".join(f"{c:>9}" for c in COLUMNS))
    if args[:1] == ["--file"]:
        out, violations = analyze(parse(args[1]))
        print(row(pathlib.Path(args[1]).name, out))
        for n, contexts in violations[:10]:
            print(f"  VIOLATION nonce={n} {contexts}")
        return
    total = collections.Counter()
    for arg in args:
        run = pathlib.Path(arg)
        for node in ("n1", "n2", "n3"):
            path = run / node / "nonce-oracle.log"
            if not path.exists():
                print(f"{run.name + ' ' + node:44} no nonce-oracle.log")
                continue
            out, violations = analyze(parse(path))
            total.update(out)
            print(row(f"{run.name} {node}", out))
            for n, contexts in violations[:5]:
                print(f"  VIOLATION nonce={n} {contexts}")
    if len(args) > 1:
        print(row("total", total))


if __name__ == "__main__":
    main()
