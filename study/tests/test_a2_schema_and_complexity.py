"""Supplementary H1-review fixes:

  1. The A2 arm is a valid `arm` in the published schema, and analyze.py ingests A2 rows
     cleanly (A2 is outside the H1/H3 contrast family -- it just must load/validate).
  2. The analysis dataframe carries a `complexity_bin`/`complexity_score` column joined
     from TASKS.lock.json, ready for the later H4 (paradigm x complexity) model.
  3. load_rows() SKIPS harness-fault rows at load time, so a quarantined row can never
     reach -- and depress -- any agent statistic.
"""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)
sys.path.insert(0, os.path.join(STUDY, "analysis"))

import jsonschema                                                    # noqa: E402
import pytest                                                        # noqa: E402

import analyze                                                       # noqa: E402
from harness.schema import RESULTS_JSON_SCHEMA, ResultRow, validate_row  # noqa: E402

PUBLISHED = os.path.join(STUDY, "config", "results_schema.json")
TASKS = os.path.join(STUDY, "config", "TASKS.lock.json")


def _a2_row():
    return ResultRow(
        run_id="p4_fanout__A2__seed1", task="p4_fanout", arm="A2", seed=1,
        spark_version="4.1.0", image_digest="uncontainerized", git_sha="abc",
        base_model_id="claude-opus-4-8", executor_config={"instances": 4},
        silent_defect=False, defect_classes=[], detection_stage="n/a",
        iterations=2, wall_s=3.0, executor_seconds=None, usd=0.01, exit_class="completed")


# ---------------------------------------------------------------------------
# 1. A2 arm is schema-valid
# ---------------------------------------------------------------------------
def test_a2_arm_in_schema_enum():
    assert "A2" in RESULTS_JSON_SCHEMA["properties"]["arm"]["enum"]
    with open(PUBLISHED) as f:
        assert "A2" in json.load(f)["properties"]["arm"]["enum"]


def test_a2_row_validates_against_runtime_and_published_schema():
    d = json.loads(_a2_row().to_json())
    assert validate_row(d) == [], validate_row(d)            # runtime validator
    with open(PUBLISHED) as f:
        jsonschema.validate(d, json.load(f))                 # PUBLISHED contract: must not raise


def test_analyze_ingests_a2_rows_without_blowing_up():
    rows = [
        {"task": "p4_fanout", "seed": 1, "arm": "A", "exit_class": "completed",
         "silent_defect": True, "reached_correct": True,
         "executor_seconds_to_correct": 1.0, "usd": 0.01,
         "failing_iterations": 0, "dry_run_intercepts": 0},
        {"task": "p4_fanout", "seed": 1, "arm": "A2", "exit_class": "completed",
         "silent_defect": False, "reached_correct": True,
         "executor_seconds_to_correct": 1.0, "usd": 0.01,
         "failing_iterations": 0, "dry_run_intercepts": 0},
    ]
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "r.jsonl")
        with open(p, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        rep = analyze.build_report(p, arms_meta=None, tasks_path=TASKS)
    # A2 loads + appears in the per-arm table (descriptive); it is simply absent from the
    # fixed H1 contrast family, which must not raise.
    assert "A2" in rep["H1_per_arm"], rep["H1_per_arm"].keys()
    assert rep["H1_per_arm"]["A2"]["n"] == 1
    assert all("A2" not in key for key in rep["H1_contrasts"])   # not in contrast family


# ---------------------------------------------------------------------------
# 2. complexity column joined from TASKS.lock
# ---------------------------------------------------------------------------
def test_analysis_frame_carries_complexity_column():
    rows = [{"task": "p4_fanout", "seed": 1, "arm": "A", "silent_defect": False},
            {"task": "p5_mart", "seed": 1, "arm": "A", "silent_defect": True}]
    analyze.join_complexity(rows, TASKS)
    # joined onto the rows from the AUTHORITATIVE frozen corpus (TASKS.lock.json v3):
    # both tasks carry an explicit complexity_bin of "Low" in the lock, which the join
    # honours over the bare defect-count heuristic; complexity_score is the in-scope
    # defect count (p4_fanout=5, p5_mart=2 in the merged corpus).
    assert rows[0]["complexity_bin"] == "low" and rows[0]["complexity_score"] == 5
    assert rows[1]["complexity_bin"] == "low" and rows[1]["complexity_score"] == 2
    # ...and present as a column in the analysis dataframe for the later H4 model.
    df = analyze.analysis_frame(rows)
    assert df is not None, "pandas should be available in this env"
    assert "complexity_bin" in df.columns and "complexity_score" in df.columns
    assert set(df["complexity_bin"]) == {"low"}


def test_build_report_exposes_complexity_join():
    rows = [{"task": "p4_fanout", "seed": 1, "arm": "A", "exit_class": "completed",
             "silent_defect": False, "reached_correct": True,
             "executor_seconds_to_correct": 1.0, "usd": 0.0,
             "failing_iterations": 0, "dry_run_intercepts": 0}]
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "r.jsonl")
        with open(p, "w") as f:
            f.write(json.dumps(rows[0]) + "\n")
        rep = analyze.build_report(p, arms_meta=None, tasks_path=TASKS)
    cj = rep["meta"]["complexity_join"]
    assert cj["n_rows_with_complexity"] == 1
    # p4_fanout carries an explicit complexity_bin "Low" in the merged frozen corpus.
    assert cj["by_bin"].get("low") == 1


# ---------------------------------------------------------------------------
# 3. load-time quarantine filter
# ---------------------------------------------------------------------------
def test_load_rows_skips_harness_fault_rows_at_load_time():
    rows = [
        {"task": "t", "seed": 1, "arm": "A", "exit_class": "completed", "silent_defect": False},
        {"task": "t", "seed": 1, "arm": "B", "exit_class": "HARNESS_ERROR", "silent_defect": False},
        {"task": "t", "seed": 2, "arm": "B", "exit_class": "PROPOSE_TIMEOUT", "silent_defect": False},
        {"task": "t", "seed": 3, "arm": "B", "exit_class": "HARNESS_EXCEPTION", "silent_defect": False},
    ]
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "r.jsonl")
        with open(p, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        loaded = analyze.load_rows(p)
        every = analyze.load_all_rows(p)
    # load_rows() drops ALL harness-fault classes; only the genuine agent row survives.
    assert len(loaded) == 1 and loaded[0]["exit_class"] == "completed"
    assert all(not analyze.is_harness_fault_row(r) for r in loaded)
    # load_all_rows() keeps everything (for the quarantine appendix).
    assert len(every) == 4


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
