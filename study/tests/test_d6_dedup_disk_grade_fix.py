"""Regression test for the LOCAL IMPERATIVE D6 secondary-dedup-table read-back bug.

This is the natural completion of the imperative output read-back fix
(test_imperative_readback_fix.py): extend the SAME 'grade from disk, not the live
session catalog' philosophy to the D6 SECONDARY dedup-table check.

BUG (confounds imperative-vs-SDP on exactly the 2 tasks with a D6 check on a SEPARATE
dedup table -- orders_silver_gold/silver_orders and p12_quarantine_dlq/clean_orders):
the oracle's D6 check graded that separate dedup table by reading it through the LIVE
session catalog (executor.read_table). For LOCAL IMPERATIVE arms the agent materializes
that table as an in-session TEMP VIEW and then calls `spark.stop()` (idiomatic). The
revived session cannot see the temp view (it is not on disk), so the D6 grade failed
spuriously with REQUIRED_OUTPUT_TABLE_NOT_FOUND on a perfectly valid run.

FIX (two parts, mirroring the primary gold read-back fix):
  * HARNESS: for LOCAL IMPERATIVE arms, the separate dedup table is materialized to its
    OWN parquet path (AGENT_DEDUP_PATH, a sibling of AGENT_OUTPUT_PATH) and graded from
    DISK via read_path -- never the live catalog. SDP / remote Connect arms grade in an
    isolated subprocess whose session is alive and KEEP their live-catalog read.
  * AGENT CONTRACT: the imperative prompt requires the dedup table written as parquet to
    AGENT_DEDUP_PATH (a real on-disk dataset), not only a temp view. Location only; the
    semantic target ('a deduplicated table') is unchanged and the clarification leaks no
    table name / purpose / defect label.

The 4 already-fine D6 tasks (p2/p6/p10/p13) grade a table that IS the primary output
(dedup_table == table), so they already read from disk and are NOT routed through the
new dedup path -- proven by test_dedup_path_only_for_separate_dedup_tables below.

Spark-dependent tests skip (not fail) when pyspark is unavailable.
"""
import json
import os
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import runner                                       # noqa: E402
from harness.backends.base import LoopState                      # noqa: E402
from harness.runner import local_imperative_dedup_path           # noqa: E402


# ---------------------------------------------------------------------------
# (1) PURE-UNIT: dedup-path derivation -- the exact scope of the fix. No Spark.
# ---------------------------------------------------------------------------
def test_dedup_path_only_for_separate_dedup_tables():
    """The on-disk dedup path is produced ONLY when the contract's dedup_table DIFFERS
    from the primary table (the 2 affected tasks). For same-table D6 (p2/p6/p10/p13 and
    friends) and for SDP/Connect (empty primary path) it is "" -- so those paths are
    untouched: same-table D6 already grades the primary output from disk, and SDP/Connect
    keep their live-catalog read."""
    ws = "/work/orders_silver_gold__A__seed42"
    primary = os.path.join(ws, "gold_daily.parquet")

    # SEPARATE dedup table -> sibling parquet path under the same workspace. The
    # filename is NEUTRAL (does not embed the table name) to stay defect-neutral.
    sep = {"table": "gold_daily", "dedup_table": "silver_orders"}
    assert local_imperative_dedup_path(sep, primary) == os.path.join(ws, "secondary_output.parquet")
    sep2 = {"table": "gold_daily", "dedup_table": "clean_orders"}
    assert local_imperative_dedup_path(sep2, primary) == os.path.join(ws, "secondary_output.parquet")

    # SAME-table D6 (p6/new_merge_upsert etc.): no separate path -> "".
    assert local_imperative_dedup_path({"table": "silver_orders", "dedup_table": "silver_orders"}, primary) == ""
    # current-state tasks (p2/p10/p13): dedup_table == table -> "".
    assert local_imperative_dedup_path({"table": "customers_current", "dedup_table": "customers_current"}, primary) == ""
    # no dedup_table declared -> "".
    assert local_imperative_dedup_path({"table": "gold_daily"}, primary) == ""
    # SDP / remote Connect: primary path is "" -> "" (never routed to disk).
    assert local_imperative_dedup_path(sep, "") == ""
    assert local_imperative_dedup_path(None, primary) == ""


# ---------------------------------------------------------------------------
# (2) PURE-UNIT: the imperative contract is symmetric + defect-neutral. No Spark.
# ---------------------------------------------------------------------------
def _arms():
    from harness.arm_manifest import load_arms
    return load_arms(os.path.join(STUDY, "arms"))


def test_imperative_dedup_contract_is_disk_pinned_symmetric_and_defect_neutral():
    """With a dedup path set, the imperative prompt pins the additional table to
    AGENT_DEDUP_PATH as parquet (symmetric with the primary AGENT_OUTPUT_PATH), and
    leaks NO table name / purpose / defect label. SDP arms never see it."""
    from harness.backends.live import AnthropicBrain
    arms = _arms()
    brain = AnthropicBrain("claude-sonnet-4-6", "TASK")

    state = LoopState(task="orders_silver_gold", seed=42, workspace="/ws",
                      dataset_path="/data/orders.ndjson", output_table="gold_daily",
                      output_path="/ws/gold_daily.parquet",
                      dedup_path="/ws/secondary_output.parquet")  # harness uses a NEUTRAL filename
    msg = brain._user_message(state, arms["A"])
    # symmetric, location-only disk pin for both outputs
    assert "AGENT_OUTPUT_PATH=/ws/gold_daily.parquet" in msg
    assert "AGENT_DEDUP_PATH=/ws/secondary_output.parquet" in msg
    assert "as parquet" in msg
    assert "do NOT call saveAsTable" in msg
    # defect-neutral: the harness instruction must not name the table or its purpose
    for forbidden in ("D6", "dedup", "silver_orders", "clean_orders", "createOrReplaceTempView"):
        assert forbidden not in msg, f"imperative dedup prompt leaked {forbidden!r}: {msg}"

    # SDP (B/B1) must NEVER see the local imperative disk contract, even if dedup_path leaks in.
    for arm_id in ("B", "B1"):
        sdp_msg = brain._user_message(state, arms[arm_id])
        assert "AGENT_DEDUP_PATH" not in sdp_msg
        assert "AGENT_OUTPUT_PATH" not in sdp_msg


def test_imperative_contract_unchanged_when_no_separate_dedup_table():
    """Tasks WITHOUT a separate dedup table keep the original temp-view contract
    BYTE-FOR-BYTE (no AGENT_DEDUP_PATH), so their paradigm comparison is unconfounded."""
    from harness.backends.live import AnthropicBrain
    arms = _arms()
    brain = AnthropicBrain("claude-sonnet-4-6", "TASK")
    state = LoopState(task="p6_dedup_watermark", seed=42, workspace="/ws",
                      dataset_path="/data/orders.ndjson", output_table="silver_orders",
                      output_path="/ws/silver_orders.parquet")  # dedup_path defaults to ""
    msg = brain._user_message(state, arms["A"])
    assert "AGENT_DEDUP_PATH" not in msg
    assert "createOrReplaceTempView" in msg
    assert "AGENT_OUTPUT_PATH=/ws/silver_orders.parquet" in msg


# ---------------------------------------------------------------------------
# (3) SPARK: build_output_profile grades the dedup table from DISK for imperative,
#     and from the LIVE CATALOG for SDP (read_table) -- proving SDP is un-regressed.
# ---------------------------------------------------------------------------
_ORDERS_NDJSON = "\n".join(json.dumps(r) for r in [
    # order A: two events; latest (event_time 200) carries amount 20.00 / cat z
    {"order_id": "A", "merchant_id": "m", "event_time": "100", "amount": "10.00", "category": "x"},
    {"order_id": "A", "merchant_id": "m", "event_time": "200", "amount": "20.00", "category": "z"},
    # order B: single event
    {"order_id": "B", "merchant_id": "m", "event_time": "150", "amount": "5.00", "category": "y"},
]) + "\n"

# truth (latest-by-event_time survivor): A -> (20.00, z), B -> (5.00, y)
_CONTRACT = {"table": "gold_daily", "revenue_col": "revenue", "date_col": "event_date",
             "substrate": "orders", "dedup_table": "silver_orders", "key_col": "order_id",
             "payload_cols": ["amount", "category"]}


def _write_inputs(tmp):
    inp = os.path.join(tmp, "orders.ndjson")
    with open(inp, "w") as f:
        f.write(_ORDERS_NDJSON)
    return inp


def _build_dedup_parquets(spark, tmp):
    """A CORRECT dedup parquet (matches latest-by-event_time truth) and a BAD one."""
    from pyspark.sql import Row
    good = os.path.join(tmp, "silver_orders.parquet")
    bad = os.path.join(tmp, "silver_orders_bad.parquet")
    spark.createDataFrame([Row(order_id="A", amount="20.00", category="z"),
                           Row(order_id="B", amount="5.00", category="y")]) \
         .write.mode("overwrite").parquet(good)
    # BAD: order A keeps the EARLIER row (10.00/x) -> an arbitrary (non-latest) survivor.
    spark.createDataFrame([Row(order_id="A", amount="10.00", category="x"),
                           Row(order_id="B", amount="5.00", category="y")]) \
         .write.mode("overwrite").parquet(bad)
    return good, bad


def test_build_output_profile_grades_dedup_from_disk_not_catalog():
    """LOCAL IMPERATIVE: D6 grades the dedup parquet from DISK (read_path + dedup_path),
    independent of the session catalog. A correct table reconciles (0 arbitrary
    survivors); a bad one fires D6. The catalog is never consulted for the dedup table."""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from harness import output_oracles
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master("local[1]").appName("d6-disk")
             .config("spark.sql.catalogImplementation", "in-memory")
             .config("spark.ui.enabled", "false").getOrCreate())
    try:
        with tempfile.TemporaryDirectory() as tmp:
            inp = _write_inputs(tmp)
            good, bad = _build_dedup_parquets(spark, tmp)
            # a minimal primary output so the primary read succeeds (only D6 in scope)
            primary = os.path.join(tmp, "gold_daily.parquet")
            spark.read.parquet(good).withColumnRenamed("amount", "amt").write \
                 .mode("overwrite").parquet(primary)

            def read_path(p):
                return spark.read.parquet(p)

            def exploding_read_table(name):  # the dedup table is NOT in the catalog
                raise AssertionError(f"read_table({name!r}) must NOT be used on the disk path")

            ok = output_oracles.build_output_profile(
                exploding_read_table, spark, inp, ["D6"], _CONTRACT,
                read_path=read_path, output_path=primary, dedup_path=good)
            assert "d6_read_error" not in ok.extra, ok.extra
            assert "required_output_read_error" not in ok.extra, ok.extra
            assert ok.extra.get("d6_dedup_path") == good
            assert ok.d6_ambiguous_keys_unhandled == 0, ok.extra.get("d6")

            defective = output_oracles.build_output_profile(
                exploding_read_table, spark, inp, ["D6"], _CONTRACT,
                read_path=read_path, output_path=primary, dedup_path=bad)
            assert defective.extra.get("d6_dedup_path") == bad
            assert defective.d6_ambiguous_keys_unhandled >= 1, defective.extra.get("d6")
    finally:
        spark.stop()


def test_build_output_profile_sdp_still_grades_dedup_via_live_catalog():
    """SDP / remote Connect path is UN-REGRESSED: with NO read_path/dedup_path (exactly
    how connect_helper.output_profile and build_output_profile_subprocess call it), D6 is
    still read from the LIVE catalog via read_table -- the dedup table as a session view,
    never a parquet path. The new disk branch is inert for SDP."""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from harness import output_oracles
    from pyspark.sql import SparkSession, Row

    spark = (SparkSession.builder.master("local[1]").appName("d6-sdp")
             .config("spark.sql.catalogImplementation", "in-memory")
             .config("spark.ui.enabled", "false").getOrCreate())
    try:
        with tempfile.TemporaryDirectory() as tmp:
            inp = _write_inputs(tmp)
            # SDP-style: the agent's output tables live in the (alive, subprocess) catalog.
            spark.createDataFrame([Row(event_date="2024-01-01", category="z", revenue=20.0),
                                   Row(event_date="2024-01-01", category="y", revenue=5.0)]) \
                 .createOrReplaceTempView("gold_daily")
            spark.createDataFrame([Row(order_id="A", amount="20.00", category="z"),
                                   Row(order_id="B", amount="5.00", category="y")]) \
                 .createOrReplaceTempView("silver_orders")  # correct latest-by-time survivor

            seen = {}

            def read_table(name):
                seen[name] = seen.get(name, 0) + 1
                return spark.table(name)

            prof = output_oracles.build_output_profile(
                read_table, spark, inp, ["D6"], _CONTRACT)  # NO read_path / dedup_path
            # the dedup table was read from the CATALOG (read_table), not a disk path
            assert seen.get("silver_orders", 0) >= 1, seen
            assert "d6_dedup_path" not in prof.extra
            assert "d6_read_error" not in prof.extra, prof.extra
            assert prof.d6_ambiguous_keys_unhandled == 0, prof.extra.get("d6")
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# (4) SPARK end-to-end: an imperative arm on orders_silver_gold / p12_quarantine_dlq
#     reaches a REAL D6 grade from disk AFTER the agent calls spark.stop().
# ---------------------------------------------------------------------------
_HEADER = """
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
STR = StructType([StructField("order_id",StringType()),StructField("merchant_id",StringType()),
                  StructField("event_time",StringType()),StructField("amount",StringType()),
                  StructField("category",StringType())])
"""

# Deterministic latest-by-event_time survivor -> a valid dedup table. build() returns
# (gold, dedup); the footer writes BOTH as parquet (dedup to AGENT_DEDUP_PATH) and then
# calls spark.stop() -- proving the grade reads the dedup table back from DISK.
_D6_CLEAN = _HEADER + """
def build(spark, input_path):
    spark.conf.set("spark.sql.session.timeZone","UTC")
    df = spark.read.text(input_path).select(F.from_json("value", STR).alias("s")).select("s.*")
    is_epoch = F.col("event_time").rlike("^[0-9]+$")
    ts = F.when(is_epoch, F.timestamp_millis(F.col("event_time").cast("long"))).otherwise(F.to_timestamp("event_time"))
    w = Window.partitionBy("order_id").orderBy(F.col("_ts").desc_nulls_last())
    dedup = (df.withColumn("_ts", ts).filter(F.col("order_id").isNotNull())
               .withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1)
               .select("order_id","amount","category"))
    base = df.withColumn("amt", F.col("amount").cast("double")).withColumn("event_date", F.to_date(ts))
    gold = base.filter(F.col("amt").isNotNull() & F.col("event_date").isNotNull()).groupBy(
        "event_date","category").agg(F.sum("amt").alias("revenue"),
        F.first("order_id", ignorenulls=True).alias("order_id"),
        F.first("amount", ignorenulls=True).alias("amount"))
    return gold, dedup
if __name__ == "__main__":
    import os, sys
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    gold, dedup = build(spark, os.environ.get("AGENT_INPUT_PATH",""))
    if "--analyze-only" in sys.argv:
        gold.schema; dedup.schema; print("dry-run analyzed OK"); sys.exit(0)
    dedup_dest = os.environ.get("AGENT_DEDUP_PATH") or os.path.join(
        os.path.dirname(os.environ["AGENT_OUTPUT_PATH"]), "secondary_output.parquet")
    dedup.write.mode("overwrite").parquet(dedup_dest)
    gold.write.mode("overwrite").parquet(os.environ["AGENT_OUTPUT_PATH"])
    spark.stop()                       # idiomatic teardown -> grade MUST read from DISK
    print("Run is COMPLETED")
"""


# Writes ONLY the primary gold output -- NOT the dedup table -- then stops the session.
# Stands in for an iteration that fails to (re)write the contract's dedup table.
_D6_PRIMARY_ONLY = _HEADER + """
def build(spark, input_path):
    spark.conf.set("spark.sql.session.timeZone","UTC")
    df = spark.read.text(input_path).select(F.from_json("value", STR).alias("s")).select("s.*")
    is_epoch = F.col("event_time").rlike("^[0-9]+$")
    ts = F.when(is_epoch, F.timestamp_millis(F.col("event_time").cast("long"))).otherwise(F.to_timestamp("event_time"))
    base = df.withColumn("amt", F.col("amount").cast("double")).withColumn("event_date", F.to_date(ts))
    return base.filter(F.col("amt").isNotNull() & F.col("event_date").isNotNull()).groupBy(
        "event_date","category").agg(F.sum("amt").alias("revenue"),
        F.first("order_id", ignorenulls=True).alias("order_id"),
        F.first("amount", ignorenulls=True).alias("amount"))
if __name__ == "__main__":
    import os
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    build(spark, os.environ.get("AGENT_INPUT_PATH","")).write.mode("overwrite").parquet(
        os.environ["AGENT_OUTPUT_PATH"])
    spark.stop()                       # NOTE: never writes AGENT_DEDUP_PATH
    print("Run is COMPLETED")
"""


def test_stale_dedup_parquet_cannot_mask_a_non_writing_iteration():
    """A valid dedup parquet from iteration 1 must NOT be graded for iteration 2 when
    iteration 2 fails to (re)write it. run_execute clears the dedup path BEFORE each
    execute (the same idiom as the primary GOLD output), so a non-writing iteration
    surfaces the real REQUIRED_OUTPUT_TABLE_NOT_FOUND instead of a stale-clean pass.

    PRE-FIX (no dedup cleanup) the iteration-1 parquet survives and the oracle grades
    that stale file clean; POST-FIX iteration 2's execute removes it first, so the grade
    fails as it should."""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from harness import output_oracles
    from harness.backends.base import Proposal
    from harness.backends.local import LocalSparkExecutor

    with tempfile.TemporaryDirectory() as tmp:
        inp = _write_inputs(tmp)
        ws = os.path.join(tmp, "ws"); os.makedirs(ws)
        out_path = os.path.join(ws, "gold_daily.parquet")
        dedup_path = os.path.join(ws, "secondary_output.parquet")
        ex = LocalSparkExecutor(out_table="gold_daily", warehouse_dir=os.path.join(tmp, "wh"),
                                ui_port=4092)
        st = LoopState(task="orders_silver_gold", seed=0, workspace=ws, dataset_path=inp,
                       output_table="gold_daily", output_path=out_path, dedup_path=dedup_path)
        try:
            # iteration 1: write BOTH the primary and the dedup parquet.
            with open(os.path.join(ws, "pipeline.py"), "w") as f:
                f.write(_D6_CLEAN)
            o1 = ex.run_execute(Proposal(iteration=0, code=_D6_CLEAN, command="python"), None, st)
            assert o1.completed, o1.log
            assert os.path.isdir(dedup_path), "iteration 1 did not materialize the dedup parquet"

            # iteration 2: write ONLY the primary -> the dedup parquet is NOT (re)written.
            with open(os.path.join(ws, "pipeline.py"), "w") as f:
                f.write(_D6_PRIMARY_ONLY)
            o2 = ex.run_execute(Proposal(iteration=1, code=_D6_PRIMARY_ONLY, command="python"), None, st)
            assert o2.completed, o2.log   # the primary IS written, so execute completes

            # The stale dedup parquet from iteration 1 must be GONE (cleared pre-execute).
            assert not os.path.exists(dedup_path), \
                "stale dedup parquet survived into iteration 2 -- it could mask a failure"

            # And the D6 grade must now FAIL (required output not found), not stale-clean.
            prof = output_oracles.build_output_profile(
                ex.read_table, ex.spark, inp, ["D6"], _CONTRACT,
                read_path=ex.read_output_path, output_path=out_path, dedup_path=dedup_path)
            assert prof.extra.get("required_output_read_error"), prof.extra
            assert "d6" not in prof.extra, prof.extra
        finally:
            ex.stop()


def test_dedup_path_scope_matches_the_real_corpus():
    """Pin the disk-grade scope to the ACTUAL task catalog: only the tasks whose
    dedup_table differs from the primary table produce a non-empty dedup path; every
    other corpus task yields "". A future task that adds a SEPARATE dedup table trips
    this assertion instead of silently changing imperative grading behaviour.
    (corpus22 / v3.0.0 added new_lineitem_reconcile, whose dedup mart is a secondary
    output; acknowledged here.)"""
    catalog = os.path.join(STUDY, "config", "TASKS.lock.json")
    if not os.path.exists(catalog):
        pytest.skip(f"task catalog not found at {catalog}")
    with open(catalog) as f:
        tasks = json.load(f).get("tasks", [])
    assert tasks, "no tasks found in the corpus catalog"
    expected = {"orders_silver_gold", "p12_quarantine_dlq", "new_lineitem_reconcile"}
    got = set()
    for t in tasks:
        contract = t.get("output_contract") or {}
        primary = f"/work/{t['id']}__A__seed0/{contract.get('table', 'out')}.parquet"
        path = local_imperative_dedup_path(contract, primary)
        if path:
            got.add(t["id"])
            assert path.endswith("secondary_output.parquet"), path
    assert got == expected, f"dedup-path scope drifted from the corpus: {got} != {expected}"


def test_imperative_arm_reaches_real_d6_grade_from_disk_after_spark_stop():
    """End-to-end through runner.run_cell on the EXACT 2 affected tasks: the agent writes
    the dedup table to disk and calls spark.stop(); the run is graded as COMPLETED with a
    real D6 grade read from disk.

    PRE-FIX this returned task_success=False with error_class REQUIRED_OUTPUT_TABLE_NOT_FOUND
    -- the grade read the dedup table through the dead session catalog. POST-FIX the run
    completes and D6 is graded from the on-disk parquet."""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from harness import cost as costmod
    from harness.backends.local import LocalSparkExecutor, ScriptedBrain

    arm = _arms()["A"]   # imperative, no gate
    with tempfile.TemporaryDirectory() as tmp:
        wh = os.path.join(tmp, "wh")

        def cfg():
            return runner.StudyConfig(
                base_model_id="claude-sonnet-4-6",
                task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
                executor_config=costmod.ExecutorConfig(4, 4, 16.0, 0.192, "local", "local"),
                generator="generators/gen_messy_orders.py")

        for task_id, dedup_table in (("orders_silver_gold", "silver_orders"),
                                     ("p12_quarantine_dlq", "clean_orders")):
            contract = {"table": f"gold_{task_id}", "revenue_col": "revenue",
                        "date_col": "event_date", "substrate": "orders",
                        "dedup_table": dedup_table, "key_col": "order_id",
                        "payload_cols": ["amount", "category"]}
            task_spec = {"id": task_id, "defects_in_scope": ["D6"],
                         "input": "generators/gen_messy_orders.py", "output_contract": contract}

            def make_brain(task, a, seed):
                return ScriptedBrain([{"code": _D6_CLEAN, "command": "python", "rationale": "d6_clean"}])

            def make_executor(task, a, seed):
                return LocalSparkExecutor(out_table=contract["table"], warehouse_dir=wh, ui_port=4090)

            row = runner.run_cell(task_spec, arm, 42, cfg(), make_brain, make_executor,
                                  work_dir=tmp, clock=1750000000.0)

            # The agent called spark.stop(); the grade still reached the dedup table on disk.
            assert row.task_success is True, (task_id, row.exit_class, row.per_iteration)
            assert row.exit_class == "completed", (task_id, row.exit_class)
            last = row.per_iteration[-1].get("execute", {})
            assert last.get("error_class") != "REQUIRED_OUTPUT_TABLE_NOT_FOUND", (task_id, last)
            # the on-disk dedup parquet exists under the cell workspace (sibling of gold)
            wsdir = os.path.dirname(row.transcript_path)
            assert os.path.isdir(os.path.join(wsdir, "secondary_output.parquet")), os.listdir(wsdir)
        # path I/O only: no Hive metastore artifacts
        assert not os.path.exists(os.path.join(tmp, "metastore_db"))


def test_agent_dedup_path_env_injected_only_for_separate_dedup_tasks():
    """local.py injects AGENT_DEDUP_PATH iff state.dedup_path is set, and otherwise the
    key is absent -- so single-output tasks get byte-for-byte the same env as before."""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from harness.backends.base import Proposal
    from harness.backends.local import LocalSparkExecutor

    probe = (
        "import os\n"
        "open(os.environ['PROBE_OUT'],'w').write(os.environ.get('AGENT_DEDUP_PATH','<unset>'))\n"
        "from pyspark.sql import SparkSession\n"
        "SparkSession.builder.getOrCreate().createDataFrame([(1,)],['x'])"
        ".write.mode('overwrite').parquet(os.environ['AGENT_OUTPUT_PATH'])\n"
        "print('Run is COMPLETED')\n")

    with tempfile.TemporaryDirectory() as tmp:
        for case, dedup_path in (("with", os.path.join(tmp, "d", "silver_orders.parquet")),
                                  ("without", "")):
            ws = os.path.join(tmp, f"ws_{case}"); os.makedirs(ws)
            with open(os.path.join(ws, "pipeline.py"), "w") as f:
                f.write(probe)
            probe_out = os.path.join(tmp, f"probe_{case}.txt")
            os.environ["PROBE_OUT"] = probe_out
            ex = LocalSparkExecutor(out_table="gold", warehouse_dir=os.path.join(tmp, "wh"),
                                    ui_port=4091)
            st = LoopState(task="t", seed=1, workspace=ws, dataset_path="unused",
                           output_table="gold", output_path=os.path.join(ws, "gold.parquet"),
                           dedup_path=dedup_path)
            try:
                ex.run_execute(Proposal(iteration=0, code=probe, command="python"), None, st)
            finally:
                ex.stop()
                os.environ.pop("PROBE_OUT", None)
            seen = open(probe_out).read()
            if case == "with":
                assert seen == dedup_path, f"AGENT_DEDUP_PATH not injected: {seen!r}"
            else:
                assert seen == "<unset>", f"AGENT_DEDUP_PATH leaked when no dedup table: {seen!r}"
