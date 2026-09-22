"""Corpus-v3 quantifier tests: nested-D8, UDF classifier, HC cross-stage truth (§9).

Each new/changed defect has a deterministic ground-truth quantifier; this pins
them on fixed seeds and proves they SEPARATE a correct output from a defective one
(the floor-effect property: there is signal, and a correct build is not impossible).
Requires pyspark; skips cleanly without it.
"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)


def _find_repo_root():
    # Mirrors harness/runner.py: study/ sits two levels deep in the paper repo,
    # three in the original layout. Walk up to the dir holding generators/ or .git.
    env = os.environ.get("STUDY_REPO_ROOT")
    if env:
        return os.path.abspath(env)
    d = HERE
    for _ in range(6):
        d = os.path.dirname(d)
        if os.path.isdir(os.path.join(d, "generators")) or os.path.isdir(os.path.join(d, ".git")):
            return d
    return os.path.normpath(os.path.join(STUDY, "..", ".."))


REPO = _find_repo_root()
_DB_CANDIDATES = (os.path.join(REPO, "defect_battery"),
                  os.path.join(REPO, "experiments", "defect_battery"))
DB = next((p for p in _DB_CANDIDATES if os.path.isdir(p)), _DB_CANDIDATES[0])


def _load(name, rel):
    path = os.path.join(REPO, rel)
    if not os.path.exists(path):
        path = os.path.join(DB, os.path.basename(rel))
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _have_spark():
    try:
        import pyspark  # noqa: F401
        return True
    except Exception:
        return False


def _spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("quantify_v3_test")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def _gen(gen_rel, args, out):
    with open(out, "w") as fo, open(out + ".prof", "w") as fe:
        subprocess.run([sys.executable, os.path.join(REPO, gen_rel)] + args,
                       stdout=fo, stderr=fe, check=True)
    return out


# --- nested D8 (line_items array-of-structs) --------------------------------
def test_d8_nested_separates_v3_from_v2():
    if not _have_spark():
        print("SKIP d8_nested: no pyspark"); return
    q = _load("dbq3", "experiments/defect_battery/quantify.py")
    with tempfile.TemporaryDirectory() as td:
        v3 = _gen("generators/gen_messy_orders.py", ["--seed", "42", "--v3"], os.path.join(td, "v3.ndjson"))
        v2 = _gen("generators/gen_messy_orders.py", ["--seed", "42"], os.path.join(td, "v2.ndjson"))
        sp = _spark()
        try:
            n3, d3 = q.q_d8_nested(sp, v3)
            n2, _ = q.q_d8_nested(sp, v2)
            assert n3 == 203, n3
            assert d3["nested_dollars_silently_excluded"] > 1000.0, d3
            assert n2 == 0, n2          # v2 stream has no line_items -> backward compatible
        finally:
            sp.stop()
    print(f"d8_nested: v3={n3} rows /${d3['nested_dollars_silently_excluded']:.2f}, v2={n2}")


# --- UDF email classifier ----------------------------------------------------
def test_udf_classifier_truth_and_grading():
    if not _have_spark():
        print("SKIP udf: no pyspark"); return
    qu = _load("dbqu", "experiments/defect_battery/quantify_udf.py")
    # pure label function: deterministic on the documented cases
    assert qu.true_category(None) == "routing"
    assert qu.true_category("   ") == "routing"
    assert qu.true_category("緊急: サーバー停止") == "urgent"
    assert qu.true_category("URGENT: prod down") == "urgent"
    assert qu.true_category("FREE prize winner") == "spam"
    assert qu.true_category("RE: ticket 1") == "routing"
    assert qu.true_category("Weekly newsletter") == "info"
    with tempfile.TemporaryDirectory() as td:
        em = _gen("generators/gen_emails.py", ["--seed", "42"], os.path.join(td, "em.ndjson"))
        sp = _spark()
        try:
            opp, det = qu.q_udf(sp, em)
            assert opp == 203 and det["null_or_empty_subject_rows"] == 122
            assert det["nonascii_subject_rows"] == 81
            assert sum(det["true_label_distribution"].values()) == det["total_rows"] == 900
            subs = qu._read_subjects(sp, em)
            # a CORRECT classifier mislabels nothing; a NAIVE one (null->spam,
            # non-ASCII->info) mislabels exactly the opportunity rows.
            correct = [(e, s, qu.true_category(s)) for e, s in subs]
            n_correct, _ = qu.grade_classified(sp, em, correct)
            def naive(s):
                if s is None or not s.strip():
                    return "spam"
                cf = s.casefold()
                if any(k in cf for k in qu.ASCII_URGENT):
                    return "urgent"
                if any(k in cf for k in qu.ASCII_SPAM):
                    return "spam"
                if any(k in cf for k in qu.ASCII_ROUTING):
                    return "routing"
                return "info"
            naive_out = [(e, s, naive(s)) for e, s in subs]
            n_naive, dn = qu.grade_classified(sp, em, naive_out)
            assert n_correct == 0, n_correct
            assert n_naive == 203, (n_naive, dn)
        finally:
            sp.stop()
    print(f"udf: opportunity={opp}, correct_misclass={n_correct}, naive_misclass={n_naive}")


# --- HC cross-stage ground truth --------------------------------------------
def test_hc_ground_truth_invariants():
    if not _have_spark():
        print("SKIP hc: no pyspark"); return
    hc = _load("dbqhc", "experiments/defect_battery/quantify_hc.py")
    with tempfile.TemporaryDirectory() as td:
        tr = _gen("generators/gen_trades.py", ["--seed", "42"], os.path.join(td, "tr.ndjson"))
        ck = _gen("generators/gen_clickstream.py", ["--seed", "42"], os.path.join(td, "ck.ndjson"))
        sp = _spark()
        try:
            pos = hc.hc1_truth_positions(sp, tr)
            assert len(pos) == 10 and all(v > 0 for v in pos.values()), pos
            # a defective mart that drops one currency fails reconciliation.
            bad = {k: v for k, v in pos.items() if k != "EUR"}
            ok, _ = hc.hc1_reconcile(bad, pos)
            assert ok is False
            ok2, _ = hc.hc1_reconcile(dict(pos), pos)
            assert ok2 is True
            # SCD2 overlap predicate catches an injected overlap.
            overlap_rows = [
                {"currency": "EUR", "effective_from": "2026-06-20", "effective_to": "2026-06-22"},
                {"currency": "EUR", "effective_from": "2026-06-21", "effective_to": None},
            ]
            assert hc.scd2_overlap_count(overlap_rows) == 1
            clean_rows = [
                {"currency": "EUR", "effective_from": "2026-06-20", "effective_to": "2026-06-21"},
                {"currency": "EUR", "effective_from": "2026-06-21", "effective_to": None},
            ]
            assert hc.scd2_overlap_count(clean_rows) == 0
            t2 = hc.hc2_truth(sp, ck)
            assert t2["n_clean"] + t2["n_malformed"] == t2["n_source_lines"]
            f = t2["funnel_unique_users"]
            assert f["view"] >= f["cart"] >= f["checkout"] >= f["purchase"] > 0
            ok3, _ = hc.hc2_event_accounting(t2["n_clean"], t2["n_malformed"], t2["n_source_lines"])
            assert ok3 is True
            ok4, _ = hc.hc2_event_accounting(t2["n_clean"], 0, t2["n_source_lines"])
            assert ok4 is False    # dropping the DLQ breaks the no-loss invariant
        finally:
            sp.stop()
    print(f"hc: hc1 currencies={len(pos)}, hc2 funnel={t2['funnel_unique_users']}")


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1; print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
