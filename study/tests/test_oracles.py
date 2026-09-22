"""Validate the BLIND oracle grader against known ground truth.

Three layers of validation:

  1. E3 numbers (requires pyspark): the grader's quantifiers, applied to the
     seed=42 dataset, reproduce the registered oracle numbers EXACTLY
     (D2=246, D7=275, D8=250 / $49,778.06, D6=0). This proves the grader shares
     a single source of truth with the E3 battery and cannot have drifted.
  2. Classification logic (pure, no Spark): RunOutcome fixtures exercise every
     branch of grade_run -- structural caught at dry_run, semantic shipped,
     semantic mitigated, latent D6, never-completed -- with asserted
     silent_defect / detection_stage.
  3. Blindness + pilots: assert grade_run takes NO `arm` parameter, and that the
     two real pilot runs (both shipped correct output) grade as silent_defect
     False (the capability-retention negative control).

Run directly (`python tests/test_oracles.py`) or under pytest.
"""
import inspect
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
sys.path.insert(0, STUDY)

from harness import oracles  # noqa: E402


# ---------------------------------------------------------------------------
# layer 2: classification logic (no Spark)
# ---------------------------------------------------------------------------
def test_structural_caught_at_dry_run():
    spec = oracles.TaskOracleSpec("t", ["D1", "D8"])
    out = oracles.RunOutcome(
        completed=False,
        analysis_log="[UNRESOLVED_COLUMN.WITH_SUGGESTION] ... SQLSTATE: 42703",
    )
    g = oracles.grade_run(spec, out)
    assert g.silent_defect is False, "structural caught pre-execution is not silent"
    assert g.per_defect_detection["D1"] == "dry_run"
    assert g.detection_stage == "dry_run"


def test_semantic_shipped_is_silent():
    spec = oracles.TaskOracleSpec("t", ["D8"])
    prof = oracles.OutputProfile(d8_rows_dropped=250, d8_dollars_dropped=49778.06)
    out = oracles.RunOutcome(completed=True, output=prof)
    g = oracles.grade_run(spec, out)
    assert g.silent_defect is True
    assert g.defect_classes == ["D8"]
    assert g.detection_stage == "never"
    assert g.detail["D8"]["dollars"] == 49778.06


def test_semantic_mitigated_not_silent():
    spec = oracles.TaskOracleSpec("t", ["D2", "D7", "D8"])
    prof = oracles.OutputProfile()  # all zero residual = agent fixed everything
    out = oracles.RunOutcome(completed=True, output=prof)
    g = oracles.grade_run(spec, out)
    assert g.silent_defect is False
    assert g.defect_classes == []
    assert g.detection_stage == "n/a"


def test_latent_d6_not_silent():
    spec = oracles.TaskOracleSpec("t", ["D6"])
    prof = oracles.OutputProfile(d6_ambiguous_keys_unhandled=0)  # latent on seed=42
    out = oracles.RunOutcome(completed=True, output=prof)
    g = oracles.grade_run(spec, out)
    assert g.silent_defect is False, "D6 is latent on this dataset -> not silent"


def test_never_completed_no_silent():
    spec = oracles.TaskOracleSpec("t", ["D8"])
    out = oracles.RunOutcome(completed=False)
    g = oracles.grade_run(spec, out)
    assert g.silent_defect is False
    assert g.detection_stage == "n/a"


def test_mixed_structural_and_semantic():
    # caught D4 structurally at dry-run but still shipped D8 silently
    spec = oracles.TaskOracleSpec("t", ["D4", "D8"])
    prof = oracles.OutputProfile(d8_rows_dropped=250)
    out = oracles.RunOutcome(
        completed=True,
        analysis_log="[TABLE_OR_VIEW_NOT_FOUND] ... SQLSTATE: 42P01",
        output=prof,
    )
    g = oracles.grade_run(spec, out)
    assert g.per_defect_detection["D4"] == "dry_run"
    assert g.silent_defect is True
    assert g.defect_classes == ["D8"]
    assert g.detection_stage == "never"  # a silent ship dominates the run-level summary


# ---------------------------------------------------------------------------
# layer 3: blindness + pilots
# ---------------------------------------------------------------------------
def test_grader_is_blind_to_arm():
    sig = inspect.signature(oracles.grade_run)
    assert "arm" not in sig.parameters, "grade_run must not receive the arm label"
    # RunOutcome must not carry the arm OR any gate-revealing hint
    rfields = set(oracles.RunOutcome.__dataclass_fields__)
    for forbidden in ("arm", "base_model", "skills", "dry_run_gate", "safety_skill",
                      "structural_caught_stage", "paradigm", "gate"):
        assert forbidden not in rfields, f"RunOutcome leaks arm/gate info via {forbidden!r}"
    # grade_run's source must not reference a gate field either
    import inspect as _i
    src = _i.getsource(oracles.grade_run)
    assert "structural_caught_stage" not in src, "grade_run still references a gate hint"


def test_pilots_grade_non_silent():
    """Both real pilots shipped correct output -> silent_defect False."""
    spec = oracles.TaskOracleSpec("orders_silver_gold", ["D1", "D2", "D3", "D6", "D7", "D8"])
    # sandbox-a (Arm A): correct output, reconciles
    a = oracles.RunOutcome(completed=True, output=oracles.OutputProfile(reconciles=True))
    ga = oracles.grade_run(spec, a)
    assert ga.silent_defect is False and ga.defect_classes == []
    # sandbox-b (Arm B): correct output, gate intercepted a transient framework issue
    b = oracles.RunOutcome(
        completed=True,
        analysis_log="[TABLE_OR_VIEW_NOT_FOUND] silver_valid",  # transient, not a pre-reg defect in scope
        output=oracles.OutputProfile(reconciles=True),
    )
    gb = oracles.grade_run(spec, b)
    assert gb.silent_defect is False and gb.defect_classes == []


# ---------------------------------------------------------------------------
# layer 1: E3 numbers (requires pyspark) -- run as a subprocess to isolate Spark
# ---------------------------------------------------------------------------
EXPECTED = {"D2": 246, "D6": 0, "D7": 275, "D8": 250}
EXPECTED_D8_DOLLARS = 49778.06


def _gen_seed42(path):
    gen = os.path.join(REPO, "generators", "gen_messy_orders.py")
    with open(path, "w") as fo:
        subprocess.run([sys.executable, gen, "--seed", "42"], stdout=fo,
                       stderr=subprocess.DEVNULL, check=True)


def test_e3_numbers_reproduced():
    try:
        import pyspark  # noqa: F401
    except Exception:
        print("SKIP test_e3_numbers_reproduced: pyspark not importable")
        return
    with tempfile.TemporaryDirectory() as td:
        ds = os.path.join(td, "orders.ndjson")
        _gen_seed42(ds)
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.master("local[2]").appName("oracle_test")
                 .config("spark.ui.enabled", "false")
                 .config("spark.sql.shuffle.partitions", "4").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        try:
            prof = oracles.OutputProfile.from_quantifiers(spark, ds, ["D2", "D6", "D7", "D8"])
            assert prof.d2_misparsed_rows == EXPECTED["D2"], prof.d2_misparsed_rows
            assert prof.d6_ambiguous_keys_unhandled == EXPECTED["D6"], prof.d6_ambiguous_keys_unhandled
            assert prof.d7_wrong_day_rows == EXPECTED["D7"], prof.d7_wrong_day_rows
            assert prof.d8_rows_dropped == EXPECTED["D8"], prof.d8_rows_dropped
            assert abs(prof.d8_dollars_dropped - EXPECTED_D8_DOLLARS) < 0.01, prof.d8_dollars_dropped
        finally:
            spark.stop()
    print(f"E3 numbers reproduced: D2={prof.d2_misparsed_rows} D6={prof.d6_ambiguous_keys_unhandled} "
          f"D7={prof.d7_wrong_day_rows} D8={prof.d8_rows_dropped}/${prof.d8_dollars_dropped:.2f}")


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
