"""Validate the EXTENDED quantifiers (CDC + payments substrates) on fixed seeds.

Same discipline as test_oracles.py: each new quantifier reproduces a known-good
number on a fixed seed, so the second/third substrates are pinned the same way
the orders battery is (D2=246, D7=275, D8=250/$49,778.06).

Known-good (locked) numbers -- corpus v3 (daily FX + wider basket + tombstones):
  cdc_d6  @ customers_cdc seed=7  -> 77 ambiguous customers (non-latent D6)
  pay_d7  @ payments seed=42      -> 1066 rows on the wrong UTC day
  pay_d8  @ payments seed=42      -> 1136 foreign rows / $276,498.91 USD dropped
The CDC generator's documented oracle is preserved (total_events=263,
distinct=100, deleted=13 at seed=7) -- v3 only nulls the delete payloads; the
payments numbers move because v3 widens the foreign basket (exotic codes) and
spreads the stream over ~4 UTC days with a per-day FX rate (generators/fx.py).

Run directly (`python tests/test_oracles_ext.py`) or under pytest.
"""
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

# import the extended quantifiers the same single-source way the grader does
import importlib.util  # noqa: E402

_QEXT_CANDIDATES = (os.path.join(REPO, "defect_battery", "quantify_ext.py"),
                    os.path.join(REPO, "experiments", "defect_battery", "quantify_ext.py"))
QEXT_PATH = next((p for p in _QEXT_CANDIDATES if os.path.exists(p)), _QEXT_CANDIDATES[0])
_spec = importlib.util.spec_from_file_location("quantify_ext", QEXT_PATH)
qext = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(qext)

CDC_D6_EXPECTED = 77
PAY_D7_EXPECTED = 1066
PAY_D8_ROWS_EXPECTED = 1136
PAY_D8_DOLLARS_EXPECTED = 276498.91
CDC_TOTAL_EVENTS = 263


def _gen(gen_rel, args, out):
    gen = os.path.join(REPO, gen_rel)
    with open(out, "w") as fo, open(out + ".profile", "w") as fe:
        subprocess.run([sys.executable, gen] + args, stdout=fo, stderr=fe, check=True)
    return out


def _spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("oracle_ext_test")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def test_cdc_generator_oracle_preserved():
    with tempfile.TemporaryDirectory() as td:
        out = _gen("generators/gen_customers_cdc.py", [], os.path.join(td, "cdc.ndjson"))
        n = sum(1 for _ in open(out))
        assert n == CDC_TOTAL_EVENTS, f"CDC seed=7 events {n} != {CDC_TOTAL_EVENTS}"
        prof = open(out + ".profile").read()
        assert '"distinct_customers": 100' in prof, prof


def test_cdc_d6_reproduced():
    try:
        import pyspark  # noqa: F401
    except Exception:
        print("SKIP test_cdc_d6_reproduced: pyspark not importable")
        return
    with tempfile.TemporaryDirectory() as td:
        ds = _gen("generators/gen_customers_cdc.py", [], os.path.join(td, "cdc.ndjson"))
        spark = _spark()
        try:
            affected, detail = qext.q_cdc_d6(spark, ds)
            assert affected == CDC_D6_EXPECTED, f"cdc_d6 {affected} != {CDC_D6_EXPECTED}"
            assert detail["ambiguous_keys_conflicting_payload"] == CDC_D6_EXPECTED
        finally:
            spark.stop()
    print(f"cdc_d6 reproduced: {affected} ambiguous customers (non-latent D6)")


def test_payments_quantifiers_reproduced():
    try:
        import pyspark  # noqa: F401
    except Exception:
        print("SKIP test_payments_quantifiers_reproduced: pyspark not importable")
        return
    with tempfile.TemporaryDirectory() as td:
        ds = _gen("generators/gen_payments.py", [], os.path.join(td, "pay.ndjson"))
        spark = _spark()
        try:
            d7, _ = qext.q_pay_d7(spark, ds)
            assert d7 == PAY_D7_EXPECTED, f"pay_d7 {d7} != {PAY_D7_EXPECTED}"
            d8_rows, d8_detail = qext.q_pay_d8(spark, ds)
            assert d8_rows == PAY_D8_ROWS_EXPECTED, f"pay_d8 rows {d8_rows} != {PAY_D8_ROWS_EXPECTED}"
            dollars = d8_detail["usd_equivalent_silently_excluded"]
            assert abs(dollars - PAY_D8_DOLLARS_EXPECTED) < 0.01, dollars
        finally:
            spark.stop()
    print(f"pay_d7 reproduced: {d7} wrong-UTC-day rows")
    print(f"pay_d8 reproduced: {d8_rows} rows / ${dollars:.2f} USD silently excluded")


def test_ext_quantifiers_are_arm_agnostic():
    import inspect
    for fn in (qext.q_cdc_d6, qext.q_pay_d7, qext.q_pay_d8):
        params = list(inspect.signature(fn).parameters)
        assert params == ["spark", "path"], f"{fn.__name__} signature leaks beyond (spark,path): {params}"


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
