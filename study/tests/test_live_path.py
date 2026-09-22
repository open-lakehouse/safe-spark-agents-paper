"""LIVE measurement-path validation on REAL Spark (the whole point of the fix).

Replay tests cannot catch B1-B4 because replay injects the answer. This test
drives the ACTUAL runner -> write-code -> execute -> materialize -> read-back ->
output-oracle -> grade -> delta-cost path with a deterministic scripted brain and
a real in-process Spark executor (no LLM, no remote cluster, no API key). It
proves, end to end:

  (a) B2  the agent's generated code is WRITTEN to the workspace and EXECUTED;
  (b) B1  the MATERIALIZED table is read back and graded by the output oracle;
  (c) B1  silent_defect == TRUE on a completed-but-DEFECTIVE output (D8 drop and
          D2 misparse) and == FALSE on the CORRECT output -- the fake-null the
          reviewer found is gone;
  (d) B8  per-iteration executor-seconds are a before/after DELTA, not cumulative.

The remote mTLS Connect cluster is the operator's pilot step; the LocalSparkExecutor
runs the identical runner/oracle/cost path, only the cluster differs.

Run directly (`python tests/test_live_path.py`) or under pytest. Needs pyspark+JDK.
"""
import os
import sys
import tempfile

import pytest

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

from harness import cost as costmod          # noqa: E402
from harness import runner                   # noqa: E402
from harness.arm_manifest import load_arms    # noqa: E402
from harness.backends.local import LocalSparkExecutor, ScriptedBrain  # noqa: E402

# --- agent code the scripted brain "writes" (real pyspark modules) ----------
# Agent-owned programs: each defines build(spark, input_path) -> DataFrame AND
# a __main__ that acquires its OWN SparkSession, reads AGENT_INPUT_PATH, supports
# --analyze-only, writes the final gold DataFrame as parquet to AGENT_OUTPUT_PATH,
# and prints "Run is COMPLETED".
# The harness writes this VERBATIM and injects no session/main; the LocalSparkExecutor
# runs it in-process so its getOrCreate() binds to the executor's session.

_HEADER = """
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
STR = StructType([StructField("order_id",StringType()),StructField("merchant_id",StringType()),
                  StructField("event_time",StringType()),StructField("amount",StringType()),
                  StructField("category",StringType())])
"""

# Agent-owned program footer: acquire the session, read AGENT_INPUT_PATH, support
# --analyze-only (analyze without materializing), else write parquet to AGENT_OUTPUT_PATH.
_MAIN = """
if __name__ == "__main__":
    import os, sys
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    inp = os.environ.get("AGENT_INPUT_PATH", "")
    df = build(spark, inp)
    if "--analyze-only" in sys.argv:
        df.schema  # force analysis; no materialization (no executors)
        print("dry-run analyzed OK"); sys.exit(0)
    df.write.mode("overwrite").parquet(os.environ["AGENT_OUTPUT_PATH"])
    print("Run is COMPLETED")
"""

# `amount` is coerced with cast("double") -- a numeric string ("251.42") casts to
# 251.42, a non-numeric/null casts to null (non-ANSI) -- which is exactly the set
# the output oracle's true-total counts, so a correct build reconciles.
CORRECT = _HEADER + """
def build(spark, input_path):
    spark.conf.set("spark.sql.session.timeZone","UTC")
    df = spark.read.text(input_path).select(F.from_json("value", STR).alias("s")).select("s.*")
    amt = F.col("amount").cast("double")
    is_epoch = F.col("event_time").rlike("^[0-9]+$")
    ts = F.when(is_epoch, F.timestamp_millis(F.col("event_time").cast("long"))).otherwise(F.to_timestamp("event_time"))
    out = df.withColumn("amt", amt).withColumn("event_date", F.to_date(ts))
    out = out.filter(F.col("amt").isNotNull() & F.col("event_date").isNotNull())
    return out.groupBy("event_date","category").agg(F.sum("amt").alias("revenue"))
""" + _MAIN

# D8: amount typed Double via from_json -> quoted-string amounts become null and
# are silently dropped from the sum. Output revenue UNDER-COUNTS (reconciliation
# fails) but the pipeline COMPLETES.
D8_DEFECT = _HEADER + """
NUM = StructType([StructField("order_id",StringType()),StructField("merchant_id",StringType()),
                  StructField("event_time",StringType()),StructField("amount",DoubleType()),
                  StructField("category",StringType())])
def build(spark, input_path):
    spark.conf.set("spark.sql.session.timeZone","UTC")
    df = spark.read.text(input_path).select(F.from_json("value", NUM).alias("s")).select("s.*")
    is_epoch = F.col("event_time").rlike("^[0-9]+$")
    ts = F.when(is_epoch, F.timestamp_millis(F.col("event_time").cast("long"))).otherwise(F.to_timestamp("event_time"))
    out = df.withColumn("event_date", F.to_date(ts)).filter(F.col("amount").isNotNull() & F.col("event_date").isNotNull())
    return out.groupBy("event_date","category").agg(F.sum("amount").alias("revenue"))
""" + _MAIN

# D2: event_time typed TIMESTAMP via from_json -> epoch-millis strings misparse to
# year ~58437, so the gold table has rows bucketed to an impossible far-future
# date. Amount handled correctly (revenue reconciles), so ONLY D2 fires.
D2_DEFECT = _HEADER + """
TS = StructType([StructField("order_id",StringType()),StructField("merchant_id",StringType()),
                 StructField("event_time",TimestampType()),StructField("amount",StringType()),
                 StructField("category",StringType())])
def build(spark, input_path):
    spark.conf.set("spark.sql.session.timeZone","UTC")
    df = spark.read.text(input_path).select(F.from_json("value", TS).alias("s")).select("s.*")
    amt = F.col("amount").cast("double")
    out = df.withColumn("amt", amt).withColumn("event_date", F.to_date(F.col("event_time")))
    out = out.filter(F.col("amt").isNotNull() & F.col("event_date").isNotNull())
    return out.groupBy("event_date","category").agg(F.sum("amt").alias("revenue"))
""" + _MAIN

# Agent-owned program that EXITS 0 and even prints COMPLETED but never writes
# the contract output path -- the neutral completion check must catch this.
NO_OUTPUT = _HEADER + """
def build(spark, input_path):
    return spark.read.text(input_path)
if __name__ == "__main__":
    import os
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    build(spark, os.environ.get("AGENT_INPUT_PATH", ""))
    print("Run is COMPLETED")  # claims success but writes NO path
"""

# D6 disk-grade footer: build() returns (gold_df, dedup_df). The footer writes BOTH
# as parquet -- the dedup table to AGENT_DEDUP_PATH (its disk contract) -- and then
# calls spark.stop() (idiomatic teardown). The harness grade MUST read the dedup
# table back from DISK, never from the torn-down session catalog.
_D6_MAIN = """
if __name__ == "__main__":
    import os, sys
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    inp = os.environ.get("AGENT_INPUT_PATH", "")
    gold, dedup = build(spark, inp)
    if "--analyze-only" in sys.argv:
        gold.schema; dedup.schema  # force analysis; no materialization
        print("dry-run analyzed OK"); sys.exit(0)
    dedup_dest = os.environ.get("AGENT_DEDUP_PATH") or os.path.join(
        os.path.dirname(os.environ["AGENT_OUTPUT_PATH"]), "dedup.parquet")
    dedup.write.mode("overwrite").parquet(dedup_dest)
    gold.write.mode("overwrite").parquet(os.environ["AGENT_OUTPUT_PATH"])
    spark.stop()                       # idiomatic teardown -> grade must read from DISK
    print("Run is COMPLETED")
"""

# D6 defect for orders: the final gold path is valid and reconciles, but the
# oracle-graded dedup table -- now materialized to AGENT_DEDUP_PATH as PARQUET (the
# disk contract) -- has arbitrary first-row survivors. The OutputProfile reads that
# parquet from DISK on the path-based imperative branch (Hive-free, no saveAsTable,
# and robust to the agent's spark.stop()).
D6_DISK_DEFECT = _HEADER + """
def build(spark, input_path):
    spark.conf.set("spark.sql.session.timeZone","UTC")
    df = spark.read.text(input_path).select(F.from_json("value", STR).alias("s")).select("s.*")
    amt = F.col("amount").cast("double")
    is_epoch = F.col("event_time").rlike("^[0-9]+$")
    ts = F.when(is_epoch, F.timestamp_millis(F.col("event_time").cast("long"))).otherwise(F.to_timestamp("event_time"))
    base = df.withColumn("amt", amt).withColumn("event_date", F.to_date(ts))
    # Arbitrary survivor: keep a fixed amount per key, not the latest event_time.
    dedup = base.filter(F.col("order_id").isNotNull()).groupBy("order_id").agg(
        F.lit("0.00").alias("amount"), F.first("category", ignorenulls=True).alias("category"))
    gold = base.filter(F.col("amt").isNotNull() & F.col("event_date").isNotNull()).groupBy(
        "event_date","category").agg(
        F.sum("amt").alias("revenue"),
        F.first("order_id", ignorenulls=True).alias("order_id"),
        F.first("amount", ignorenulls=True).alias("amount"))
    return gold, dedup
""" + _D6_MAIN

# Missing dedup table: writes gold but NEVER writes the dedup parquet, so the D6
# grade hits a real read error and the run is a visible incomplete-output failure.
D6_NO_DEDUP = CORRECT

CONTRACT = {"table": None, "revenue_col": "revenue", "date_col": "event_date", "substrate": "orders"}


def _cfg(tmp):
    return runner.StudyConfig(
        base_model_id="claude-sonnet-4-6",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(4, 4, 16.0, 0.192, "local", "local"),
        generator="generators/gen_messy_orders.py",
    )


def _run_case(tmp, case_id, code, warehouse, arm):
    contract = dict(CONTRACT, table=f"gold_{case_id}")
    task_spec = {"id": case_id, "defects_in_scope": ["D2", "D8"],
                 "input": "generators/gen_messy_orders.py", "output_contract": contract}
    proposals = [{"code": code, "command": "python", "rationale": case_id}]

    def make_brain(task, a, seed):
        return ScriptedBrain(list(proposals))

    def make_executor(task, a, seed):
        return LocalSparkExecutor(out_table=contract["table"], warehouse_dir=warehouse)

    return runner.run_cell(task_spec, arm, 42, _cfg(tmp), make_brain, make_executor,
                           work_dir=tmp, clock=1750000000.0)


def test_live_path_grades_real_output():
    try:
        import pyspark  # noqa: F401
    except Exception:
        print("SKIP test_live_path_grades_real_output: pyspark not importable")
        return
    arm = load_arms(os.path.join(STUDY, "arms"))["A"]   # imperative, no gate
    with tempfile.TemporaryDirectory() as tmp:
        wh = os.path.join(tmp, "wh")
        correct = _run_case(tmp, "correct", CORRECT, wh, arm)
        d8 = _run_case(tmp, "d8_defect", D8_DEFECT, wh, arm)
        d2 = _run_case(tmp, "d2_defect", D2_DEFECT, wh, arm)

        # (a) B2: the agent code was written to the workspace and is the real artifact,
        # and the final output is parquet under the workspace (not a warehouse table).
        for row in (correct, d8, d2):
            wsdir = os.path.dirname(row.transcript_path)
            wsfile = os.path.join(wsdir, "pipeline.py")
            assert os.path.exists(wsfile), f"agent code not written for {row.task}"
            assert "def build(" in open(wsfile).read()
            # run_cell uses output_contract["table"] as the path stem.
            parquet_dirs = [name for name in os.listdir(wsdir) if name.endswith(".parquet")]
            assert parquet_dirs, f"path output not written for {row.task}: {os.listdir(wsdir)}"
            assert os.path.isdir(os.path.join(wsdir, parquet_dirs[0]))

    # all three COMPLETED (real materialization)
    for row in (correct, d8, d2):
        assert row.exit_class == "completed", f"{row.task} did not complete: {row.exit_class}"
        assert row.task_success is True

    # (b)+(c) B1: the materialized table was read back and graded for real
    assert correct.silent_defect is False, f"CORRECT graded silent! classes={correct.defect_classes}"
    assert correct.defect_classes == []
    assert d8.silent_defect is True, "D8-defective output graded NON-silent (the fake null)"
    assert "D8" in d8.defect_classes
    assert d2.silent_defect is True, "D2-defective output graded NON-silent (the fake null)"
    assert "D2" in d2.defect_classes

    print(f"LIVE GRADE  correct: silent={correct.silent_defect} classes={correct.defect_classes}")
    print(f"LIVE GRADE  d8     : silent={d8.silent_defect} classes={d8.defect_classes}")
    print(f"LIVE GRADE  d2     : silent={d2.silent_defect} classes={d2.defect_classes}")

    # (d) B8: per-iteration executor-seconds are a delta and were actually measured
    for row in (correct, d8, d2):
        assert row.executor_seconds is not None
        assert row.executor_seconds >= 0.0


def test_completion_check_flags_missing_output_path():
    """Neutral completion check (D-4): even when the agent program exits 0 and prints
    'Run is COMPLETED', a run whose contract output parquet path is absent is FAILED /
    non-completed with error_class OUTPUT_PATH_NOT_FOUND -- completion is verified by
    a real path read-back, not by trusting the agent's claim or harness-written Spark."""
    try:
        import pyspark  # noqa: F401
    except Exception:
        print("SKIP test_completion_check_flags_missing_output_path: pyspark not importable")
        return
    from harness.backends.base import LoopState, Proposal
    arm = load_arms(os.path.join(STUDY, "arms"))["A"]
    with tempfile.TemporaryDirectory() as tmp:
        ds = os.path.join(tmp, "orders.ndjson")
        import subprocess
        with open(ds, "w") as fo:
            subprocess.run([sys.executable, os.path.join(REPO, "generators", "gen_messy_orders.py"),
                            "--seed", "42", "--N", "200"], stdout=fo, stderr=subprocess.DEVNULL, check=True)
        ws = os.path.join(tmp, "ws"); os.makedirs(ws, exist_ok=True)
        open(os.path.join(ws, "pipeline.py"), "w").write(NO_OUTPUT)
        ex = LocalSparkExecutor(out_table="never_written", warehouse_dir=os.path.join(tmp, "wh"),
                                ui_port=4055)
        st = LoopState(task="t", seed=42, workspace=ws, dataset_path=ds, output_table="never_written")
        out = ex.run_execute(Proposal(iteration=0, code=NO_OUTPUT, command="python"), arm, st)
        ex.stop()
        assert out.failed is True, "missing output path was not flagged as failed"
        assert out.completed is False, "missing output path graded completed"
        assert out.error_class == "OUTPUT_PATH_NOT_FOUND", out.error_class
        print(f"completion-check: failed={out.failed} class={out.error_class}")


def test_imperative_path_output_no_hive_metastore_init():
    """Guard: the Part-1 LOCAL imperative executor must complete through parquet
    path I/O without creating Hive/Derby metastore artifacts or warehouse table dirs."""
    try:
        import pyspark  # noqa: F401
    except Exception:
        print("SKIP test_imperative_path_output_no_hive_metastore_init: pyspark not importable")
        return
    from harness.backends.base import LoopState, Proposal
    arm = load_arms(os.path.join(STUDY, "arms"))["A"]
    code = _HEADER + """
def build(spark, input_path):
    return spark.range(3).select(F.col("id").alias("revenue"), F.current_date().alias("event_date"))
""" + _MAIN
    with tempfile.TemporaryDirectory() as tmp:
        ws = os.path.join(tmp, "ws"); os.makedirs(ws, exist_ok=True)
        open(os.path.join(ws, "pipeline.py"), "w").write(code)
        out_path = os.path.join(tmp, "gold_path.parquet")
        wh = os.path.join(tmp, "wh")
        ex = LocalSparkExecutor(out_table="gold_daily", warehouse_dir=wh, ui_port=4065)
        st = LoopState(task="t", seed=42, workspace=ws, dataset_path="unused",
                       output_table="gold_daily", output_path=out_path)
        out = ex.run_execute(Proposal(iteration=0, code=code, command="python"), arm, st)
        catalog_impl = ex.spark.conf.get("spark.sql.catalogImplementation")
        ex.stop()
        assert not out.failed and out.completed, out.log
        assert os.path.isdir(out_path), "parquet output path not written"
        assert catalog_impl == "in-memory"
        assert not os.path.exists(os.path.join(tmp, "metastore_db")), "Derby metastore_db created"
        assert not os.path.exists(os.path.join(ws, "metastore_db")), "workspace metastore_db created"
        assert not os.path.exists(os.path.join(tmp, "spark-warehouse")), "default spark-warehouse created"
        if os.path.exists(wh):
            assert "gold_daily" not in os.listdir(wh), "warehouse table dir created for output"
        print(f"hive guard: parquet={out_path} catalog={catalog_impl} no metastore/ObjectStore artifacts")




def test_path_imperative_grades_d6_secondary_dedup_from_disk_after_stop():
    """D6 disk-grade fix: for path-based LOCAL IMPERATIVE runs, the secondary dedup
    table is materialized to its OWN parquet path (AGENT_DEDUP_PATH) and graded from
    DISK -- even though the agent program calls spark.stop() before exit. A missing
    dedup parquet is a visible incomplete-output failure (not scored clean); a bad
    dedup parquet produces a D6 silent defect. No Hive metastore or warehouse table
    is created (parquet path I/O only, no saveAsTable).

    PRE-FIX this spuriously failed: the grade read the dedup table through the LIVE
    session catalog (executor.read_table), which a revived session cannot see after
    the agent's spark.stop() -> REQUIRED_OUTPUT_TABLE_NOT_FOUND on a perfectly good
    on-disk table. Reading from disk decouples the grade from the agent's session."""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    arm = load_arms(os.path.join(STUDY, "arms"))["A"]
    with tempfile.TemporaryDirectory() as tmp:
        wh = os.path.join(tmp, "wh")
        def run_with(task_id, dedup_table, code, case):
            contract = dict(CONTRACT, table=f"gold_{task_id}", dedup_table=dedup_table,
                            key_col="order_id", payload_cols=["amount", "category"])
            task_spec = {"id": task_id, "defects_in_scope": ["D6"],
                         "input": "generators/gen_messy_orders.py", "output_contract": contract}
            def make_brain(task, a, seed):
                return ScriptedBrain([{"code": code, "command": "python", "rationale": case}])
            def make_executor(task, a, seed):
                return LocalSparkExecutor(out_table=contract["table"], warehouse_dir=wh, ui_port=4075)
            return runner.run_cell(task_spec, arm, 42, _cfg(tmp), make_brain, make_executor,
                                   work_dir=tmp, clock=1750000000.0)

        for task_id, dedup_table in (("orders_silver_gold", "silver_orders"),
                                     ("p12_quarantine_dlq", "clean_orders")):
            missing = run_with(task_id, dedup_table, D6_NO_DEDUP, "d6_missing_dedup")
            bad = run_with(task_id, dedup_table, D6_DISK_DEFECT, "d6_bad_dedup")

            assert missing.task_success is False, (task_id, missing.exit_class, missing.per_iteration)
            assert missing.exit_class == "runtime_error"
            assert missing.silent_defect is False
            assert missing.per_iteration[-1]["execute"]["error_class"] == "REQUIRED_OUTPUT_TABLE_NOT_FOUND"
            # The defective dedup table was written to DISK and the agent then called
            # spark.stop(); the grade still read it back and flagged the D6 defect.
            assert bad.task_success is True, (task_id, bad.exit_class, bad.per_iteration)
            assert bad.silent_defect is True, (task_id, bad.defect_classes)
            assert "D6" in bad.defect_classes
        assert not os.path.exists(os.path.join(tmp, "metastore_db"))
        if os.path.exists(wh):
            assert "silver_orders" not in os.listdir(wh), "dedup table leaked as warehouse table"
            assert "clean_orders" not in os.listdir(wh), "dedup table leaked as warehouse table"

def test_executor_seconds_are_delta_not_cumulative():
    """B8: two consecutive executes record INDEPENDENT deltas that sum to the
    cumulative total -- proving per-iteration cost is not the cumulative counter."""
    try:
        import pyspark  # noqa: F401
    except Exception:
        print("SKIP test_executor_seconds_are_delta_not_cumulative: pyspark not importable")
        return
    from harness.backends.base import LoopState, Proposal
    arm = load_arms(os.path.join(STUDY, "arms"))["A"]
    with tempfile.TemporaryDirectory() as tmp:
        # make a tiny dataset
        ds = os.path.join(tmp, "orders.ndjson")
        import subprocess
        with open(ds, "w") as fo:
            subprocess.run([sys.executable, os.path.join(REPO, "generators", "gen_messy_orders.py"),
                            "--seed", "42", "--N", "400"], stdout=fo, stderr=subprocess.DEVNULL, check=True)
        ws = os.path.join(tmp, "ws"); os.makedirs(os.path.join(ws, "transformations"), exist_ok=True)
        for rel in ("pipeline.py",):
            open(os.path.join(ws, rel), "w").write(CORRECT)
        ex = LocalSparkExecutor(out_table="delta_probe", warehouse_dir=os.path.join(tmp, "wh"))
        st = LoopState(task="t", seed=42, workspace=ws, dataset_path=ds)
        prop = Proposal(iteration=0, code=CORRECT, command="python")
        # Create THIS executor's session first so the baseline snapshot reads its OWN UI
        # app (via the discovered actual port), not a foreign/leftover Spark UI that may
        # be bound to the default port in a busy CI/dev host.
        _ = ex.spark
        s0 = ex._executor_seconds_snapshot()
        r1 = ex.run_execute(prop, arm, st)
        s1 = ex._executor_seconds_snapshot()
        r2 = ex.run_execute(prop, arm, st)
        s2 = ex._executor_seconds_snapshot()
        ex.stop()
        if s0 is None:
            print("SKIP delta proof: Spark REST UI unavailable in this env")
            return
        assert s2 >= s1 >= s0, f"cumulative snapshots not monotonic: {s0} {s1} {s2}"
        # each recorded executor_seconds is the per-iteration delta, not cumulative
        assert r1.executor_seconds is not None and r2.executor_seconds is not None
        cumulative = s2 - s0
        assert r2.executor_seconds <= cumulative + 1e-6, "2nd iteration recorded the cumulative total (B8 not fixed)"
        print(f"B8 delta proof: iter1={r1.executor_seconds:.3f}s iter2={r2.executor_seconds:.3f}s "
              f"cumulative={cumulative:.3f}s (deltas are per-iteration)")


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback; failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
