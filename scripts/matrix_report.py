#!/usr/bin/env python3
"""One Markdown table of a matrix.yml run, from its jobs' runs artifacts.

   matrix_report.py DIR

DIR holds LABEL.runs.tsv files, one per jepsen.yml job, each with one line per
run: label, run, seed, exit status, verdict, failed checks, post-heal probe,
success rates, channel summary, store directory (jepsen.yml's Summary step
writes them). The environment gives the context: FIBER_REPOSITORY, FIBER_REF,
FIBER_SHA, RUN_URL, and MATRIX, the plan job's JSON list of scenarios, so that
a scenario whose job left no lines still gets a row.
"""
import json
import os
import pathlib
import sys

FIELDS = ["label", "run", "seed", "exit", "verdict", "failed", "probe", "rates", "channels", "store"]


def read(directory):
    """{label: [run dict]}, runs in order."""
    runs = {}
    for f in sorted(pathlib.Path(directory).glob("*.runs.tsv")):
        for line in f.read_text(errors="replace").splitlines():
            parts = line.split("\t")
            if len(parts) != len(FIELDS):
                continue
            r = dict(zip(FIELDS, parts))
            runs.setdefault(r["label"], []).append(r)
    for rs in runs.values():
        rs.sort(key=lambda r: int(r["run"]) if r["run"].isdigit() else 0)
    return runs


def valid(r):
    return r["verdict"] == "Everything looks good"


def outcome(rs):
    """A scenario's cell: how many of its runs were valid."""
    if not rs:
        return "no runs (the job failed before its first run)"
    n = sum(valid(r) for r in rs)
    mark = "✅" if n == len(rs) else ("❌" if n == 0 else "◐")
    return f"{mark} {n} of {len(rs)} valid"


def failed_checks(rs):
    """The failed checks of each invalid run, as `run: checks`."""
    cells = [f"{r['run']}: {r['failed'] if r['failed'] not in ('-', 'none') else r['verdict']}"
             for r in rs if not valid(r)]
    return "; ".join(cells) or "-"


def report(directory, env):
    runs = read(directory)
    try:
        matrix = json.loads(env.get("MATRIX") or "[]")
    except json.JSONDecodeError:
        matrix = []
    order = [m["name"] for m in matrix]
    priority = {m["name"]: m.get("priority", "") for m in matrix}
    labels = order + sorted(l for l in runs if l not in order and l != "build")
    sha = env.get("FIBER_SHA", "")
    out = []
    out.append(f"## Fault matrix: `{env.get('FIBER_REPOSITORY', '?')}@{sha[:10]}`"
               + (f" ({env['FIBER_REF']})" if env.get("FIBER_REF") and env.get("FIBER_REF") != sha else ""))
    out.append("")
    build = runs.get("build", [])
    out.append(f"- Run without faults (build job): {outcome(build) if build else 'no result'}")
    total = [r for l in labels for r in runs.get(l, [])]
    out.append(f"- Scenarios: {len(labels)}; runs: {len(total)}, of which {sum(valid(r) for r in total)} valid")
    if env.get("RUN_URL"):
        out.append(f"- Each scenario's job summary has its seeds and stores: {env['RUN_URL']}")
    out.append("")
    out.append("| Scenario | Priority | Result | Failed checks (run: checks) |")
    out.append("| --- | --- | --- | --- |")
    for l in labels:
        rs = runs.get(l, [])
        out.append(f"| {l} | {priority.get(l, '-')} | {outcome(rs)} | {failed_checks(rs)} |")
    out.append("")
    out.append("<details><summary>Every run</summary>")
    out.append("")
    out.append("| Scenario | Run | Seed | Exit | Verdict | Failed checks | Probe | Success rate | Channels |")
    out.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for l in ["build"] + labels:
        for r in runs.get(l, []):
            out.append(f"| {l} | {r['run']} | `{r['seed']}` | {r['exit']} | {r['verdict']} | {r['failed']}"
                       f" | {r['probe']} | {r['rates']} | {r['channels']} |")
    out.append("")
    out.append("</details>")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    sys.stdout.write(report(sys.argv[1] if len(sys.argv) > 1 else "runs", os.environ))
