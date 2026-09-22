#!/usr/bin/env python3
"""Merge primary run + backfill files into the final clean set (backfill wins for re-run cells),
report exit-class + remaining errors, then run analyze.py. Self-locating.
Edit PRIMARY / BACKFILL globs to match your run's filenames."""
import glob, json, os, subprocess, collections
HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.normpath(os.path.join(HERE, "..", ".."))
os.chdir(STUDY)

# primary run parts first, backfill files LAST (they win per (task,arm,seed))
PRIMARY = sorted(glob.glob("results/results.powered*.part*.jsonl")) or sorted(glob.glob("results/results.powered*.jsonl"))
BACKFILL = sorted(glob.glob("results/results.bf_*.jsonl"))
FILES = [f for f in PRIMARY if ".final." not in f] + BACKFILL
MERGED = "results/results.powered.final.jsonl"

def key(r): return ((r.get("task") or r.get("task_id")), r.get("arm"), r.get("seed"))
rows = {}
for f in FILES:
    for l in open(f):
        if l.strip():
            r = json.loads(l); rows[key(r)] = r     # later file / line wins
with open(MERGED, "w") as o:
    for r in rows.values():
        o.write(json.dumps(r) + "\n")

ec = collections.Counter(str(r.get("exit_class")) for r in rows.values())
errs = [k for k, r in rows.items() if str(r.get("exit_class")).lower() in ("harness_error", "propose_api_error")]
print(f"[merge] inputs: {FILES}")
print(f"[merge] merged cells: {len(rows)}   exit_class: {dict(ec)}")
print(f"[merge] remaining error cells ({len(errs)}): {errs}")

env = dict(os.environ)
import pyspark
env["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
subprocess.run(["python3", "analysis/analyze.py", MERGED, "--tasks", "config/TASKS.lock.json",
                "--assume-backend", "local", "--md-out", "HEADLINE.final.md",
                "--json-out", "REPORT.final.json"], env=env)
print("[merge] -> HEADLINE.final.md + REPORT.final.json")
