#!/usr/bin/env python3
"""Merge frozen A (primary run) + rerun B (fixed skill) -> the fair-skill H1 set; analyze; and print
the key convergence comparison: silent-defect rate and per-class D7, B-frozen vs B-fixed vs A."""
import json, os, subprocess
HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.normpath(os.path.join(HERE, "..", ".."))
os.chdir(STUDY)
PRIMARY = "results/results.powered.AB.n12.final.jsonl"  # frozen A + frozen B
RERUN_B = "results/results.h1rerun.B.jsonl"             # fixed-skill B
MERGED = "results/results.h1rerun.final.jsonl"

def load(p): return [json.loads(l) for l in open(p) if l.strip()] if os.path.exists(p) else []
def key(r): return ((r.get("task") or r.get("task_id")), r.get("arm"), r.get("seed"))
prim, reB = load(PRIMARY), load(RERUN_B)

# fair-skill set = frozen A  +  fixed-skill B
rows = {}
for r in prim:
    if r.get("arm") == "A":
        rows[key(r)] = r
for r in reB:
    if r.get("arm") == "B":
        rows[key(r)] = r
with open(MERGED, "w") as o:
    for r in rows.values():
        o.write(json.dumps(r) + "\n")

def rate(arm, rowset):
    xs = [r for r in rowset if r.get("arm") == arm]
    s = sum(1 for r in xs if r.get("silent_defect"))
    return s, len(xs)
def d7(arm, rowset):
    return sum(1 for r in rowset if r.get("arm") == arm
               and r.get("per_defect_detection", {}).get("D7") == "never")

A_s, A_n = rate("A", prim)                          # frozen A
Bf_s, Bf_n = rate("B", prim)                        # frozen B
Bx_s, Bx_n = rate("B", reB)                         # fixed-skill B
print(f"[merge] fair-skill set: {len(rows)} cells -> {MERGED}")
print("=== H1.3 convergence: silent-defect rate ===")
print(f"  A (frozen, bare imperative): {A_s}/{A_n} = {A_s/A_n:.3f}")
print(f"  B (frozen skill):            {Bf_s}/{Bf_n} = {Bf_s/Bf_n:.3f}")
print(f"  B (fixed skill, this rerun): {Bx_s}/{Bx_n} = {Bx_s/Bx_n:.3f}   <-- converges to A?")
print(f"=== D7 ships (arm B): frozen {d7('B', prim)}  ->  fixed {d7('B', reB)} ===")

env = dict(os.environ)
import pyspark
env["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
subprocess.run(["python3", "analysis/analyze.py", MERGED, "--tasks", "config/TASKS.lock.json",
                "--assume-backend", "local", "--md-out", "H1RERUN_HEADLINE.md",
                "--json-out", "H1RERUN_REPORT.json"], env=env)
print("[merge] -> H1RERUN_HEADLINE.md + H1RERUN_REPORT.json")
