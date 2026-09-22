"""Remote-data-staging regression guard (D-3), no network / cluster / AWS needed.

The live sweep talks to a REMOTE k8s Spark Connect cluster that cannot see this
machine's filesystem, so a bare `file:/local/...` input is PATH_NOT_FOUND. This
was invisible to all prior validation because that used the in-process local
executor (co-located FS). These tests use a FAKE Connect executor (real
ConnectExecutor.stage_input over a fake session that records copyFromLocalToFs)
to assert, for EVERY arm, that the live path:

  (a) STAGES the per-seed input over the Connect channel (copyFromLocalToFs);
  (b) REWRITES dataset_path/input_path to the staged REMOTE location -- never a
      bare local file path -- for the agent's pipeline AND the oracle;
  (c) hands the OUTPUT oracle the SAME staged path (build_output_profile.input_path);
  (d) leaves the in-process LocalSparkExecutor path UNCHANGED (no stage_input).
"""
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import cost as costmod                  # noqa: E402
from harness import output_oracles                   # noqa: E402
from harness import runner                           # noqa: E402
from harness.arm_manifest import load_arms            # noqa: E402
from harness.backends.base import ExecOutcome, GateOutcome, Proposal  # noqa: E402
from harness.backends.live import ConnectExecutor     # noqa: E402
from harness.backends.local import LocalSparkExecutor, ScriptedBrain  # noqa: E402
from harness.oracles import OutputProfile             # noqa: E402

ARMS = load_arms(os.path.join(STUDY, "arms"))
WAREHOUSE = "s3a://test-bucket/warehouse"


class _FakeWriter:
    def __init__(self, df):
        self._df = df
        self._mode = None

    def mode(self, m):
        self._mode = m
        return self

    def text(self, dest):
        self._df._spark.text_writes.append((dest, self._mode, len(self._df._rows)))


class _FakeDF:
    def __init__(self, spark, rows, schema):
        self._spark, self._rows, self._schema = spark, rows, schema

    @property
    def write(self):
        return _FakeWriter(self)


class _FakeSpark:
    """Stand-in Connect session for the createDataFrame + write.text staging recipe.
    Records createDataFrame + write.text; copyFromLocalToFs raises so a regression
    that re-introduces it is caught immediately. Never hits a network."""
    def __init__(self):
        self.created = []      # (rows, schema) from createDataFrame
        self.text_writes = []  # (dest, mode, nrows) from df.write.mode().text()

    def createDataFrame(self, data, schema):
        rows = list(data)
        self.created.append((rows, schema))
        return _FakeDF(self, rows, schema)

    def copyFromLocalToFs(self, *a, **k):
        raise AssertionError("copyFromLocalToFs must NOT be used (driver-FS only; "
                             "rejected/unreadable on the real cluster) -- regression lock")

    def table(self, name):
        return f"<fake table {name}>"


class _FakeConnect(ConnectExecutor):
    """Real stage_input/read_table (over a fake session); gate/execute stubbed so
    run_cell completes without a subprocess or cluster."""
    def __init__(self, staging_base):
        super().__init__("sc://fake:1/", None, staging_base=staging_base)
        self._spark = _FakeSpark()

    def reachable(self):
        return True

    def run_gate(self, proposal, arm, state):
        return GateOutcome(failed=False, wall_s=8.0, log="fake gate ok")

    def run_execute(self, proposal, arm, state):
        return ExecOutcome(failed=False, completed=True, wall_s=10.0,
                           executor_seconds=5.0, output_tables=[state.output_table])


def _cfg():
    return runner.StudyConfig(
        base_model_id="claude-sonnet-4-6",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(4, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://fake:1/", warehouse_uri=WAREHOUSE,
    )


# task specs covering an imperative arm-input AND an SDP-storage path
ORDERS_TASK = {"id": "orders_silver_gold", "input": "generators/gen_messy_orders.py",
               "defects_in_scope": ["D8"],
               "output_contract": {"table": "gold_daily", "revenue_col": "revenue",
                                   "substrate": "orders"}}


def _run_arm(arm, tmp, recorder):
    created = {}

    def make_brain(task, a, seed):
        code = ("@dp.table\ndef t(): return spark.read.text('x')\n"
                if a.paradigm == "sdp" else
                "def build(spark, input_path):\n    return spark.read.text(input_path)\n")
        return ScriptedBrain([{"code": code, "command": "cmd"}])

    def make_executor(task, a, seed):
        staging = f"{WAREHOUSE}/_ssa_staging/{task}/{a.arm_id}/seed{seed}"
        ex = _FakeConnect(staging)
        created["ex"] = ex
        return ex

    # record what input_path the oracle is handed (monkeypatch; restored by caller)
    def rec_profile(read_table, spark, input_path, defects, contract):
        recorder["input_path"] = input_path
        return OutputProfile()

    output_oracles.build_output_profile = rec_profile
    row = runner.run_cell(ORDERS_TASK, arm, 42, _cfg(), make_brain, make_executor,
                          work_dir=tmp, clock=1750000000.0)
    return row, created["ex"]


def test_live_path_stages_input_and_rewrites_path_every_arm():
    orig = output_oracles.build_output_profile
    try:
        for arm_id, arm in ARMS.items():
            with tempfile.TemporaryDirectory() as tmp:
                rec = {}
                row, ex = _run_arm(arm, tmp, rec)
                # the staged dest is the s3_dest DIRECTORY (write.text writes part files)
                expected_dest = f"{WAREHOUSE}/_ssa_staging/orders_silver_gold/{arm_id}/seed42"

                # (a) staged via the createDataFrame + write.text recipe to a scheme'd
                #     s3a:// dest derived from warehouse_uri -- NOT copyFromLocalToFs
                assert ex._spark.text_writes, f"arm {arm_id}: input never staged (no write.text)"
                dest, mode, nrows = ex._spark.text_writes[0]
                assert dest == expected_dest, f"arm {arm_id}: staged to {dest}, expected {expected_dest}"
                assert dest.startswith("s3a://") and "file:" not in dest and "/_data/" not in dest
                assert mode == "overwrite"
                assert nrows == 5276, f"arm {arm_id}: staged {nrows} rows (orders seed42 has 5276)"
                # the rows were shipped as a single-column 'value string' DataFrame
                created_rows, schema = ex._spark.created[0]
                assert schema == "value string"
                assert created_rows[0] == (open(_dataset_path(tmp)).read().splitlines()[0],)
                # copyFromLocalToFs (the abandoned mechanic) was NOT called -- the fake
                # session raises if it ever is, so reaching here proves it.

                # (c) the oracle was handed the STAGED s3a path, never a bare local one
                assert rec["input_path"] == expected_dest, \
                    f"arm {arm_id}: oracle got {rec['input_path']!r}, expected staged {expected_dest!r}"
                assert rec["input_path"].startswith("s3a://")

                # (b) the agent's read path is the staged s3a path, not a local file
                ws = os.path.dirname(row.transcript_path)
                if arm.paradigm == "sdp":
                    spec = open(os.path.join(ws, "spark-pipeline.yml")).read()
                    assert WAREHOUSE in spec and "file://" not in spec, \
                        f"arm {arm_id}: SDP storage is not cluster-reachable: {spec}"
                    assert os.path.exists(os.path.join(ws, "transformations", "pipeline.py"))
                else:
                    # agent-owned (D-4): pipeline.py is the agent's program VERBATIM.
                    # The staged input path is delivered at run time via AGENT_INPUT_PATH
                    # (see test_connect_imperative_env_and_argv), NOT baked into the file,
                    # so no LOCAL path may leak into the agent's module.
                    body = open(os.path.join(ws, "pipeline.py")).read()
                    assert "def build(" in body, f"arm {arm_id}: agent code not written verbatim"
                    assert "/_data/" not in body, f"arm {arm_id}: local input path leaked into pipeline.py"
                    # the staged s3a path reaches the agent via env, not a baked literal
                    assert expected_dest not in body, \
                        f"arm {arm_id}: staged path baked into pipeline.py (should be env-delivered)"
    finally:
        output_oracles.build_output_profile = orig


def test_sdp_catalog_database_threaded_studyconfig_to_spec():
    """End-to-end wiring: StudyConfig.sdp_catalog/sdp_database -> run_cell ->
    LoopState -> materialize_workspace -> _sdp_spec, for BOTH SDP arms. Proves the
    run_cell hop (cfg -> LoopState) too, not just the leaf function."""
    orig = output_oracles.build_output_profile
    output_oracles.build_output_profile = lambda *a, **k: OutputProfile()
    try:
        for arm_id in ("B", "B1"):
            arm = ARMS[arm_id]
            cfg = runner.StudyConfig(
                base_model_id="claude-sonnet-4-6",
                task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
                executor_config=costmod.ExecutorConfig(4, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
                spark_remote="sc://fake:1/", warehouse_uri=WAREHOUSE,
                sdp_catalog="my_cat", sdp_database="my_db")

            def make_brain(task, a, seed):
                return ScriptedBrain([{"code": "@dp.table\ndef t(): return spark.read.text('x')\n",
                                       "command": "cmd"}])

            def make_executor(task, a, seed):
                return _FakeConnect(f"{WAREHOUSE}/_ssa_staging/{task}/{a.arm_id}/seed{seed}")

            with tempfile.TemporaryDirectory() as tmp:
                row = runner.run_cell(ORDERS_TASK, arm, 42, cfg, make_brain, make_executor,
                                      work_dir=tmp, clock=1750000000.0)
                ws = os.path.dirname(row.transcript_path)
                spec = open(os.path.join(ws, "spark-pipeline.yml")).read()
                assert "catalog: my_cat" in spec, f"arm {arm_id}: cfg catalog not threaded:\n{spec}"
                assert "database: my_db" in spec, f"arm {arm_id}: cfg database not threaded:\n{spec}"
                # confound lock holds on the full path too
                assert "configuration:" not in spec and "timeZone" not in spec
    finally:
        output_oracles.build_output_profile = orig


def _dataset_path(tmp):
    return os.path.join(tmp, "_data", "gen_messy_orders_seed42.ndjson")


def test_connect_stage_input_unit():
    import tempfile as _t
    with _t.TemporaryDirectory() as d:
        local = os.path.join(d, "gen_messy_orders_seed42.ndjson")
        with open(local, "w") as f:
            f.write('{"a":1}\n{"a":2}\n{"a":3}\n')
        ex = _FakeConnect(f"{WAREHOUSE}/_ssa_staging/t/A/seed42")
        remote = ex.stage_input(local)
        # returns the s3_dest DIRECTORY (== staging_base), no basename appended
        assert remote == f"{WAREHOUSE}/_ssa_staging/t/A/seed42"
        assert ex._spark.text_writes == [(remote, "overwrite", 3)]
        assert ex._spark.created[0] == ([('{"a":1}',), ('{"a":2}',), ('{"a":3}',)], "value string")
        # no staging_base -> local path unchanged, nothing shipped
        ex2 = _FakeConnect(None)
        assert ex2.stage_input(local) == local
        assert ex2._spark.text_writes == [] and ex2._spark.created == []


def test_copyfromlocaltofs_not_referenced_in_stage_input():
    # regression lock: the abandoned mechanic must not be CALLED by stage_input
    import inspect
    from harness.backends.live import ConnectExecutor
    src = inspect.getsource(ConnectExecutor.stage_input)
    # it may appear in the docstring (explaining why it's abandoned) but never as a call
    code_lines = [ln for ln in src.splitlines() if not ln.strip().startswith("#")]
    assert not any(".copyFromLocalToFs(" in ln for ln in code_lines), \
        "stage_input still CALLS copyFromLocalToFs"


def test_local_executor_path_unchanged():
    # the in-process local executor exposes NO stage_input -> input is never rewritten
    assert not hasattr(LocalSparkExecutor("t", "/wh"), "stage_input")
    # _stage_input leaves the local path untouched for a local/replay executor
    class _Bare:
        pass
    assert runner._stage_input(_Bare(), "/local/orders.ndjson") == "/local/orders.ndjson"


def test_sdp_storage_local_when_no_warehouse():
    cfg = runner.StudyConfig(base_model_id="m", task_prompt_path="p",
                             executor_config=costmod.ExecutorConfig(1, 1, 1.0, 0.1))
    s = runner._sdp_storage_for(cfg, "t", "B", 42, "/ws")
    assert s == "file:///ws/pipeline-storage"
    cfg2 = runner.StudyConfig(base_model_id="m", task_prompt_path="p",
                              executor_config=costmod.ExecutorConfig(1, 1, 1.0, 0.1),
                              warehouse_uri=WAREHOUSE)
    s2 = runner._sdp_storage_for(cfg2, "t", "B", 42, "/ws")
    assert s2 == f"{WAREHOUSE}/_ssa_pipeline_storage/t/B/seed42" and "file:" not in s2


def test_connect_imperative_env_and_argv():
    """Agent-owned imperative execute (D-4): the ConnectExecutor runs the agent's
    CHOSEN command on pipeline.py with NEUTRAL env only (AGENT_INPUT_PATH /
    AGENT_OUTPUT_TABLE), injecting no session/main code."""
    from harness.backends.base import LoopState
    ex = ConnectExecutor("sc://x:1/", None)
    st = LoopState(task="t", seed=42, workspace="/ws",
                   dataset_path="s3a://b/staged", output_table="gold_daily")
    # NEUTRAL env: exactly the input path + the contract output-table name
    assert ex._imperative_env(st) == {
        "AGENT_INPUT_PATH": "s3a://b/staged", "AGENT_OUTPUT_TABLE": "gold_daily"}
    # the agent's COMMAND selects python3 vs spark-submit on pipeline.py
    py = ex._imperative_execute_argv(Proposal(0, "code", "python"), ARMS["A"], "/sh", st)
    assert py == ["python3", "/ws/pipeline.py"]
    ss = ex._imperative_execute_argv(Proposal(0, "code", "spark-submit"), ARMS["A"], "/sh", st)
    assert ss == ["/sh/bin/spark-submit", "/ws/pipeline.py"]


def test_connect_imperative_gate_runs_analyze_only():
    """B2 gate (D-4): the agent's OWN program in --analyze-only mode -- no
    harness-created _analyze_only.py session."""
    from harness.backends.base import LoopState
    ex = ConnectExecutor("sc://x:1/", None)
    st = LoopState(task="t", seed=42, workspace="/ws",
                   dataset_path="s3a://b/staged", output_table="gold_daily")
    assert ex._imperative_gate_argv(st) == ["python3", "/ws/pipeline.py", "--analyze-only"]


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
