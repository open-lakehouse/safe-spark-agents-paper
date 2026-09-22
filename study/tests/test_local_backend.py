"""Part-1 LOCAL substrate (DEVIATIONS D-7): routing + Connect-server lifecycle.

No LLM, no Spark, no network, no real JVM: every external touch (the Connect
server JVM, the port wait, the CREATE SCHEMA session) is mocked. The point is to
prove the WIRING the orchestrator's live Part-1 calibration relies on:

  * `make_local_factories` routes IMPERATIVE arms (A) to the classic local[*]
    `LocalSparkExecutor` and SDP arms (B, B1) to the local-Connect
    `LocalConnectExecutor` (the executor split that isolates the paradigm);
  * the imperative engine gets its OWN UI port (Connect UI + 1) so the two H2
    REST targets never collide;
  * `LocalConnectServer.submit_argv()` is the proven-good single-node launch;
  * `start()` spawns the JVM, waits for the gRPC port, and ensures the default
    schema; `stop()` tears the JVM down;
  * `LocalConnectExecutor.stage_input()` hands the SDP agent a `file://` path off
    the shared local FS (no S3 staging);
  * `runner.main(--backend local)` brings the server up, points the config at it,
    and always stops it (finally).
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import runner  # noqa: E402
from harness.arm_manifest import load_arms  # noqa: E402
from harness.backends import local_connect as lc  # noqa: E402
from harness.backends.local import LocalSparkExecutor  # noqa: E402
from harness.backends.local_connect import LocalConnectExecutor, LocalConnectServer  # noqa: E402

ARMS = load_arms(os.path.join(STUDY, "arms"))
# a minimal tasks_by_id: only the output_contract (for the imperative out-table) and
# a prompt (for the brain) are read by the factories.
TASKS_BY_ID = {
    "orders_silver_gold": {"id": "orders_silver_gold", "prompt": "x",
                           "output_contract": {"table": "agent_output"}},
}


def _factories(monkey_key=True):
    """Build the local factories with a FAKE (un-started) server and a dummy key."""
    if monkey_key:
        os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")
    server = SimpleNamespace(remote="sc://localhost:15002/;user_id=alice",
                             rest_url="http://localhost:4040")
    cfg = runner.StudyConfig.from_file(os.path.join(STUDY, "config", "study.config.json"))
    return runner.make_local_factories(
        cfg, "PREAMBLE", TASKS_BY_ID, server,
        imperative_warehouse="/tmp/imp_wh", imperative_ui_port=4041), server


def test_imperative_arms_route_to_local_spark():
    # B2 dropped: imperative arm B2 withdrawn to arms/supplementary per paper §6.1
    # (2026-06-29); A is the remaining active imperative arm.
    (make_brain, make_executor), _ = _factories()
    for arm_id in ("A",):
        ex = make_executor("orders_silver_gold", ARMS[arm_id], 0)
        assert isinstance(ex, LocalSparkExecutor), f"{arm_id} -> {type(ex).__name__}"
        # imperative engine on its OWN UI port (Connect UI + 1), reading the contract
        # output parquet path back for the oracle.
        assert ex.ui_port == 4041, ex.ui_port
        assert ex.out_table == "agent_output", ex.out_table


def test_local_spark_uses_in_memory_catalog_for_hive_free_path_io():
    """Classic local[*] executor must not regress to Hive/Derby metastore init."""
    class FakeBuilder:
        def __init__(self):
            self.configs = {}

        def master(self, value):
            return self

        def appName(self, value):
            return self

        def config(self, key, value):
            self.configs[key] = value
            return self

        def getOrCreate(self):
            spark = mock.Mock()
            spark.sparkContext = mock.Mock()
            spark._builder_configs = dict(self.configs)
            return spark

        def enableHiveSupport(self):
            raise AssertionError("LocalSparkExecutor must not enable Hive support")

    fake_builder = FakeBuilder()
    with mock.patch.dict(sys.modules, {
        "pyspark": mock.Mock(),
        "pyspark.sql": SimpleNamespace(SparkSession=SimpleNamespace(builder=fake_builder)),
    }):
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir="/tmp/imp_wh")
        spark = ex.spark

    assert spark._builder_configs["spark.sql.catalogImplementation"] == "in-memory"
    assert spark._builder_configs["spark.sql.warehouse.dir"] == "/tmp/imp_wh"



def test_local_spark_stops_preexisting_active_session_before_build():
    """Root-cause guard: static catalog configs are ignored by getOrCreate() when a
    classic session already exists. The imperative executor must stop that stray
    session before building its own Hive-free one."""
    class FakeBuilder:
        def __init__(self):
            self.configs = {}
        def master(self, value): return self
        def appName(self, value): return self
        def config(self, key, value):
            self.configs[key] = value
            return self
        def getOrCreate(self):
            spark = mock.Mock()
            spark.sparkContext = mock.Mock()
            spark._builder_configs = dict(self.configs)
            return spark
        def enableHiveSupport(self):
            raise AssertionError("LocalSparkExecutor must not enable Hive support")

    active = mock.Mock(name="preexisting_session")
    fake_builder = FakeBuilder()
    fake_ss = SimpleNamespace(builder=fake_builder, getActiveSession=mock.Mock(return_value=active))
    with mock.patch.dict(sys.modules, {
        "pyspark": mock.Mock(),
        "pyspark.sql": SimpleNamespace(SparkSession=fake_ss),
    }):
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir="/tmp/imp_wh")
        spark = ex.spark

    active.stop.assert_called_once()
    assert spark._builder_configs["spark.sql.catalogImplementation"] == "in-memory"



def test_local_spark_does_not_stop_active_connect_session_before_build():
    """N1: pre-build cleanup is type-aware; an active Spark Connect session belongs
    to the controller and must not be stopped as a side effect of creating the
    classic local imperative executor."""
    class FakeBuilder:
        def __init__(self):
            self.configs = {}
        def master(self, value): return self
        def appName(self, value): return self
        def config(self, key, value):
            self.configs[key] = value; return self
        def getOrCreate(self):
            spark = mock.Mock(); spark.sparkContext = mock.Mock(); spark._builder_configs = dict(self.configs); return spark
        def enableHiveSupport(self):
            raise AssertionError("LocalSparkExecutor must not enable Hive support")

    active = mock.Mock(name="active_connect_session")
    active.is_remote.return_value = True
    fake_ss = SimpleNamespace(builder=FakeBuilder(), getActiveSession=mock.Mock(return_value=active))
    with mock.patch.dict(sys.modules, {
        "pyspark": mock.Mock(),
        "pyspark.sql": SimpleNamespace(SparkSession=fake_ss),
    }):
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir="/tmp/imp_wh")
        _ = ex.spark

    active.stop.assert_not_called()


def test_existing_output_path_file_fails_gracefully_without_rmtree_crash():
    """S1: a stray file at AGENT_OUTPUT_PATH returns a failed ExecOutcome rather
    than letting shutil.rmtree raise NotADirectoryError out of the cell."""
    from harness.backends.base import LoopState, Proposal
    with tempfile.TemporaryDirectory() as tmp:
        ws = os.path.join(tmp, "ws"); os.makedirs(ws, exist_ok=True)
        out_path = os.path.join(tmp, "gold.parquet")
        open(out_path, "w").write("stray")
        ex = LocalSparkExecutor(out_table="gold_daily", warehouse_dir=os.path.join(tmp, "wh"))
        st = LoopState(task="t", seed=0, workspace=ws, dataset_path="unused",
                       output_table="gold_daily", output_path=out_path)
        out = ex.run_execute(Proposal(iteration=0, code="", command="python"), ARMS["A"], st)
        assert out.failed and not out.completed
        assert out.error_class == "OUTPUT_PATH_NOT_DIRECTORY"
        assert ex._spark is None, "file precheck should fail before starting Spark"

def test_sdp_arms_route_to_local_connect():
    (make_brain, make_executor), server = _factories()
    for arm_id in ("B", "B1"):
        ex = make_executor("orders_silver_gold", ARMS[arm_id], 0)
        assert isinstance(ex, LocalConnectExecutor), f"{arm_id} -> {type(ex).__name__}"
        # pointed at the LOCAL Connect server + its driver UI REST (H2 stage-diff).
        assert ex.spark_remote == server.remote
        assert ex.spark_rest_url == server.rest_url
        # local engine: never an S3 staging base (the FS is shared).
        assert ex.staging_base is None


def test_local_brain_is_the_live_anthropic_brain():
    from harness.backends.live import AnthropicBrain
    (make_brain, make_executor), _ = _factories()
    brain = make_brain("orders_silver_gold", ARMS["A"], 0)
    assert isinstance(brain, AnthropicBrain)
    assert brain.base_model_id == ARMS["A"].base_model_id  # SAME model as live


def test_local_factories_require_api_key():
    saved = os.environ.pop("ANTHROPIC_API_KEY", None)
    try:
        raised = False
        try:
            _factories(monkey_key=False)
        except RuntimeError as e:
            raised = True
            assert "ANTHROPIC_API_KEY" in str(e)
        assert raised, "missing API key must refuse to build local factories"
    finally:
        if saved is not None:
            os.environ["ANTHROPIC_API_KEY"] = saved


def test_submit_argv_is_the_proven_good_launch():
    srv = LocalConnectServer(port=15002, ui_port=4040, warehouse_dir="/tmp/wh",
                             spark_home="/opt/spark")
    argv = srv.submit_argv()
    assert argv[0] == "/opt/spark/bin/spark-submit"
    assert "org.apache.spark.sql.connect.service.SparkConnectServer" in argv
    assert "spark.connect.grpc.binding.port=15002" in argv
    assert "spark.ui.port=4040" in argv
    assert "spark.sql.warehouse.dir=file:///tmp/wh" in argv
    assert "spark.sql.catalogImplementation=in-memory" in argv
    assert argv[-1] == "spark-internal"


def test_remote_and_rest_url():
    srv = LocalConnectServer(port=15055, ui_port=4040, user_id="alice")
    assert srv.remote == "sc://localhost:15055/;user_id=alice"
    assert srv.rest_url == "http://localhost:4040"


def test_start_spawns_jvm_waits_and_ensures_schema():
    srv = LocalConnectServer(port=15002, ui_port=4040, warehouse_dir="/tmp/wh",
                             spark_home="/opt/spark")
    fake_proc = mock.Mock()
    with mock.patch.object(LocalConnectServer, "_port_open", return_value=False), \
         mock.patch.object(LocalConnectServer, "_wait_for_port", return_value=True), \
         mock.patch.object(srv, "ensure_schema") as ensure, \
         mock.patch("harness.backends.local_connect.subprocess.Popen",
                    return_value=fake_proc) as popen:
        srv.start(ensure_catalog="spark_catalog", ensure_database="default")
        popen.assert_called_once()
        assert popen.call_args.args[0] == srv.submit_argv()  # exact launch command
        ensure.assert_called_once_with("spark_catalog", "default")
    # stop tears the JVM down.
    srv.stop()
    fake_proc.terminate.assert_called_once()


def test_start_raises_if_port_never_comes_up():
    srv = LocalConnectServer(port=15002, ui_port=4040, warehouse_dir="/tmp/wh",
                             spark_home="/opt/spark", wait_secs=0)
    fake_proc = mock.Mock()
    with mock.patch.object(LocalConnectServer, "_port_open", return_value=False), \
         mock.patch.object(LocalConnectServer, "_wait_for_port", return_value=False), \
         mock.patch("harness.backends.local_connect.subprocess.Popen",
                    return_value=fake_proc):
        raised = False
        try:
            srv.start(ensure_database=None)
        except RuntimeError as e:
            raised = True
            assert "did not bind port" in str(e)
        assert raised, "an unreachable port must raise"
        fake_proc.terminate.assert_called_once()  # half-started proc cleaned up


def test_stage_input_returns_file_uri():
    ex = LocalConnectExecutor("sc://localhost:15002", "http://localhost:4040")
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "orders_seed7.ndjson")
        open(p, "w").close()
        staged = ex.stage_input(p)
        assert staged == f"file://{os.path.abspath(p)}"
    # already-scheme'd paths pass through unchanged.
    assert ex.stage_input("file:///x") == "file:///x"
    assert ex.stage_input("") == ""


def test_runner_main_local_brings_server_up_and_always_stops_it():
    """--backend local must start the server, point the config at it, and stop it
    in the finally (even though we select zero cells)."""
    os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")
    started = {"start": 0, "stop": 0, "cfg_remote": None, "cfg_rest": None}

    class FakeServer:
        def __init__(self, port, ui_port, warehouse_dir, log_file=None, **kw):
            self.remote = f"sc://localhost:{port}/;user_id=alice"
            self.rest_url = f"http://localhost:{ui_port}"

        def start(self, ensure_catalog="spark_catalog", ensure_database="default"):
            started["start"] += 1
            return self

        def stop(self):
            started["stop"] += 1

    real_make = runner.make_local_factories

    def spy_make(cfg, *a, **k):
        started["cfg_remote"] = cfg.spark_remote
        started["cfg_rest"] = cfg.spark_rest_url
        return real_make(cfg, *a, **k)

    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "results.jsonl")
        with mock.patch("harness.backends.local_connect.LocalConnectServer", FakeServer), \
             mock.patch.object(runner, "make_local_factories", spy_make):
            runner.main([
                "--backend", "local",
                # select NO cells: a real arm id that won't run because we restrict
                # to a non-existent task -> the loop body never executes (no network).
                "--only-tasks", "__none__",
                "--max-seeds", "1", "--out", out,
                "--work-dir", os.path.join(tmp, "work"),
                "--clock", "1750000000",
                "--local-connect-port", "15002", "--local-ui-port", "4040",
            ])
    assert started["start"] == 1, "server.start not called exactly once"
    assert started["stop"] == 1, "server.stop not called in finally"
    # the config was pointed at the local server before the factories were built.
    assert started["cfg_remote"] == "sc://localhost:15002/;user_id=alice"
    assert started["cfg_rest"] == "http://localhost:4040"


def _fake_server_class(counter, start_raises=None):
    """A FakeServer whose start()/stop() bump a shared counter; start may raise."""
    class FakeServer:
        def __init__(self, port, ui_port, warehouse_dir, log_file=None, **kw):
            self.remote = f"sc://localhost:{port}/;user_id=alice"
            self.rest_url = f"http://localhost:{ui_port}"

        def start(self, ensure_catalog="spark_catalog", ensure_database="default"):
            counter["start"] += 1
            if start_raises is not None:
                raise start_raises
            return self

        def stop(self):
            counter["stop"] += 1

    return FakeServer


def _main_local(tmp, server_cls, make_factories):
    """Run runner.main(--backend local) selecting zero cells, with the Connect
    server class and the local factories patched. Returns the raised exception (or
    None) so leak tests can assert teardown ran regardless."""
    out = os.path.join(tmp, "results.jsonl")
    with mock.patch("harness.backends.local_connect.LocalConnectServer", server_cls), \
         mock.patch.object(runner, "make_local_factories", make_factories):
        try:
            runner.main([
                "--backend", "local", "--only-tasks", "__none__",
                "--max-seeds", "1", "--out", out,
                "--work-dir", os.path.join(tmp, "work"),
                "--clock", "1750000000",
                "--local-connect-port", "15002", "--local-ui-port", "4040",
            ])
            return None
        except BaseException as e:  # noqa: BLE001
            return e


def test_leak_make_factories_failure_stops_server():
    """make_local_factories raising AFTER the server started (e.g. missing API key)
    must still tear the JVM down — the leak the cross-review flagged."""
    counter = {"start": 0, "stop": 0}

    def boom_factories(*a, **k):
        raise RuntimeError("local backend selected but ANTHROPIC_API_KEY is not set.")

    with tempfile.TemporaryDirectory() as tmp:
        err = _main_local(tmp, _fake_server_class(counter), boom_factories)
    assert isinstance(err, RuntimeError) and "ANTHROPIC_API_KEY" in str(err)
    assert counter["start"] == 1, "server must have been started"
    assert counter["stop"] == 1, f"server leaked: stop called {counter['stop']}x (want 1)"


def test_leak_start_failure_stops_server():
    """A failure during start() (e.g. ensure_schema erroring after the JVM is up)
    surfaces as start() raising; the runner's finally must still stop the server."""
    counter = {"start": 0, "stop": 0}
    server_cls = _fake_server_class(counter, start_raises=RuntimeError("ensure_schema boom"))

    def unused_factories(*a, **k):  # never reached (start raises first)
        raise AssertionError("factories should not be built when start() fails")

    with tempfile.TemporaryDirectory() as tmp:
        err = _main_local(tmp, server_cls, unused_factories)
    assert isinstance(err, RuntimeError) and "ensure_schema boom" in str(err)
    assert counter["start"] == 1
    assert counter["stop"] == 1, f"server leaked on start failure: stop {counter['stop']}x"


def test_start_self_stops_if_ensure_schema_fails():
    """Unit-level belt-and-suspenders: LocalConnectServer.start() must stop the JVM
    it just launched if ensure_schema raises, then re-raise (no leak)."""
    srv = LocalConnectServer(port=15002, ui_port=4040, warehouse_dir="/tmp/wh",
                             spark_home="/opt/spark")
    fake_proc = mock.Mock()
    with mock.patch.object(LocalConnectServer, "_port_open", return_value=False), \
         mock.patch.object(LocalConnectServer, "_wait_for_port", return_value=True), \
         mock.patch.object(srv, "ensure_schema", side_effect=RuntimeError("schema boom")), \
         mock.patch("harness.backends.local_connect.subprocess.Popen",
                    return_value=fake_proc):
        raised = False
        try:
            srv.start(ensure_database="default")
        except RuntimeError as e:
            raised = True
            assert "schema boom" in str(e)
        assert raised, "ensure_schema failure must propagate"
    fake_proc.terminate.assert_called_once()  # the launched JVM was torn down
    assert srv._proc is None  # stop() is idempotent: a second stop() is a no-op


def test_stop_is_idempotent():
    """stop() called twice must not double-terminate (real start self-stop + the
    runner finally can both fire; the second is a no-op)."""
    srv = LocalConnectServer(port=15002, ui_port=4040, spark_home="/opt/spark")
    fake_proc = mock.Mock()
    srv._proc = fake_proc
    srv.stop()
    srv.stop()
    fake_proc.terminate.assert_called_once()


class _RaisingExecutor:
    """An executor that MUST NOT be invoked: run_episode's no-code guard should skip
    the gate/execute entirely for an empty proposal. Any call here fails the test."""
    name = "raising"

    def run_gate(self, proposal, arm, state):
        raise AssertionError("run_gate called for a no-code proposal (must be skipped)")

    def run_execute(self, proposal, arm, state):
        raise AssertionError("run_execute called for a no-code proposal (must be skipped)")

    def reachable(self):
        return True

    def advance(self):
        pass


def _empty_episode(arm_id):
    from harness.backends.base import LoopState
    from harness.backends.local import ScriptedBrain
    cfg = runner.StudyConfig.from_file(os.path.join(STUDY, "config", "study.config.json"))
    brain = ScriptedBrain([{"code": "", "command": "python"}])  # never any code
    with tempfile.TemporaryDirectory() as tmp:
        state = LoopState(task="orders_silver_gold", seed=0, workspace=tmp,
                          dataset_path="ignored", output_table="agent_output")
        ep = runner.run_episode(brain, _RaisingExecutor(), ARMS[arm_id], state, cfg)
    return ep, state


def test_empty_proposal_is_graceful_failed_iteration_no_crash():
    """An empty agent proposal (no fenced code) must NOT crash the run: each
    iteration is a graceful FAILED no-code iteration, feedback is recorded, and the
    loop proceeds to max_iterations. Covers a gated arm (B) and an un-gated arm (A)
    -- in BOTH the gate/execute are skipped (the _RaisingExecutor would fire if not).
    (B2 withdrawn to arms/supplementary per paper §6.1 (2026-06-29); B is the active
    gated arm -- the no-code guard skips the gate BEFORE dispatching on paradigm, so
    the coverage is preserved.)
    """
    for arm_id in ("A", "B"):
        ep, state = _empty_episode(arm_id)
        assert not ep.completed, f"{arm_id}: a no-code run must not complete"
        assert ep.exit_class == "max_iterations", f"{arm_id}: {ep.exit_class}"
        assert len(ep.per_iteration) == ARMS[arm_id].max_iterations, \
            f"{arm_id}: ran {len(ep.per_iteration)} iters"
        for rec in ep.per_iteration:
            assert rec["execute"]["failed"] is True
            assert rec["execute"]["error_class"] == "NO_CODE_PRODUCED"
        # the agent got actionable feedback to retry, every iteration.
        assert state.feedback, f"{arm_id}: no feedback recorded"
        assert any("```python" in fb for fb in state.feedback), state.feedback
        # no-code iterations are FAILED but NOT dry-run gate intercepts ($0, 0 exec-s).
        rc = costmod_aggregate(ep)
        assert rc.failing_iterations == ARMS[arm_id].max_iterations
        assert rc.dry_run_intercepts == 0
        assert rc.total_usd == 0.0


def costmod_aggregate(ep):
    from harness import cost as costmod
    return costmod.aggregate(ep.iter_costs, ep.green_iter_index, completed=ep.completed)


def test_local_spark_executor_missing_file_is_graceful():
    """Defensive: LocalSparkExecutor must return a graceful NO_CODE_PRODUCED failure
    (never raise FileNotFoundError, never start Spark) when pipeline.py is absent."""
    from harness.backends.base import LoopState, Proposal
    with tempfile.TemporaryDirectory() as tmp:
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir=tmp, ui_port=4099)
        state = LoopState(task="t", seed=0, workspace=tmp, dataset_path="x",
                          output_table="agent_output")
        p = Proposal(iteration=0, code="", command="python")
        # B2 withdrawn to arms/supplementary per paper §6.1 (2026-06-29); run_gate is
        # arm-agnostic here (analyze-only), so A exercises the identical no-code path.
        gate = ex.run_gate(p, ARMS["A"], state)
        assert gate.failed and gate.error_class == "NO_CODE_PRODUCED", gate.error_class
        out = ex.run_execute(p, ARMS["A"], state)
        assert out.failed and not out.completed
        assert out.error_class == "NO_CODE_PRODUCED", out.error_class
        assert ex._spark is None, "no Spark session should have been created"


def test_connect_executor_missing_file_is_graceful():
    """Defensive: ConnectExecutor (SDP spark-pipeline.yml / imperative pipeline.py)
    returns NO_CODE_PRODUCED without dialing the CLI/subprocess when the file is
    absent."""
    from harness.backends.base import LoopState, Proposal
    ex = LocalConnectExecutor("sc://localhost:15002", "http://localhost:4040")
    with tempfile.TemporaryDirectory() as tmp:
        state = LoopState(task="t", seed=0, workspace=tmp, dataset_path="x",
                          output_table="agent_output")
        p = Proposal(iteration=0, code="", command="python")
        for arm_id in ("B", "A"):   # SDP checks the yml, imperative checks pipeline.py
            g = ex.run_gate(p, ARMS[arm_id], state)
            assert g.failed and g.error_class == "NO_CODE_PRODUCED", (arm_id, g.error_class)
            e = ex.run_execute(p, ARMS[arm_id], state)
            assert e.failed and not e.completed and e.error_class == "NO_CODE_PRODUCED", \
                (arm_id, e.error_class)


def test_opus_propose_has_headroom_and_streams():
    """The opus request must carry a max_tokens with real headroom for adaptive thinking
    + a code module. max_tokens raised 16000 -> 32000 (DEVIATIONS D-7): adaptive thinking
    SHARES the max_tokens budget and at 16000 exhausted it before the fenced code module
    emitted. 32000 exceeds the SDK's ~16K non-streaming long-request guard, so the call
    MUST go through the STREAMING path (client.messages.stream(...).get_final_message()),
    NOT the plain non-streaming create()."""
    from harness.backends import live
    from harness.backends.live import AnthropicBrain
    brain = AnthropicBrain(ARMS["A"].base_model_id, "prompt", sampling={})
    req = brain.build_request(system="sys", messages=[{"role": "user", "content": "x"}])
    assert req["max_tokens"] == 32000, req["max_tokens"]
    assert req["thinking"] == {"type": "adaptive"}

    # The wire call goes through messages.stream(...).get_final_message(), never create().
    fake_resp = SimpleNamespace(content=[], usage=None, stop_reason="end_turn")
    stream_cm = mock.MagicMock(name="stream_cm")
    stream_cm.__enter__.return_value.get_final_message.return_value = fake_resp
    fake_client = mock.MagicMock(name="client")
    fake_client.messages.stream.return_value = stream_cm
    with mock.patch.object(live, "_build_anthropic_client", return_value=fake_client):
        live._messages_create(req)
    fake_client.messages.stream.assert_called_once()
    stream_cm.__enter__.return_value.get_final_message.assert_called_once()
    fake_client.messages.create.assert_not_called()


def test_anthropic_client_is_constructed_with_request_timeout_and_retries():
    """ROOT CAUSE of the live hang: the Anthropic Messages call had NO HTTP timeout, so
    a stalled response blocked propose() forever (the worker stuck in ssl.recv). The
    client must be built with a bounded per-request timeout + a few retries."""
    from harness.backends import live
    from harness.backends.live import AnthropicBrain
    fake_anthropic = mock.MagicMock(name="anthropic_module")
    brain = AnthropicBrain(ARMS["A"].base_model_id, "prompt", sampling={})
    with mock.patch.dict(sys.modules, {"anthropic": fake_anthropic}):
        client = brain._client_lazy()
    assert client is fake_anthropic.Anthropic.return_value
    kwargs = fake_anthropic.Anthropic.call_args.kwargs
    assert kwargs.get("timeout") == live.ANTHROPIC_REQUEST_TIMEOUT_S, kwargs
    assert kwargs.get("max_retries") == live.ANTHROPIC_MAX_RETRIES, kwargs
    assert live.ANTHROPIC_REQUEST_TIMEOUT_S and live.ANTHROPIC_REQUEST_TIMEOUT_S > 0


def test_propose_degrades_to_no_code_when_api_call_fails():
    """A bounded API call that STILL fails (timeout/connection reset after retries) must
    NOT propagate out of propose() -- that would crash the whole sweep on one bad turn.
    It degrades to an empty proposal (stop_reason='api_error') so the run-loop's no-code
    guard advances the loop."""
    from harness.backends.base import LoopState
    from harness.backends.live import AnthropicBrain
    # A2 withdrawn to arms/supplementary per paper §6.1 (2026-06-29); A is an active
    # arm carrying the same base_model_id (identical-except-loop), so this brain-
    # behavior test is unchanged.
    brain = AnthropicBrain(ARMS["A"].base_model_id, "prompt", sampling={})
    fake_client = mock.MagicMock(name="client")
    fake_client.messages.create.side_effect = TimeoutError("read timed out")
    brain._client = fake_client
    with tempfile.TemporaryDirectory() as tmp:
        state = LoopState(task="orders_silver_gold", seed=0, workspace=tmp,
                          dataset_path="x", output_table="agent_output")
        prop = brain.propose(state, ARMS["A"])   # must NOT raise
    assert prop.code == "" and prop.command == ""
    assert prop.stop_reason == "api_error", prop.stop_reason


def test_proposal_carries_stop_reason_field():
    """stop_reason is plumbed onto Proposal so a truncated turn is visible in the
    per-iteration record."""
    from harness.backends.base import Proposal
    p = Proposal(iteration=0, code="", command="python", stop_reason="max_tokens")
    assert p.stop_reason == "max_tokens"


# ---------------------------------------------------------------------------
# Option C: the Part-1 runner process must NEVER create a Spark Connect session
# (classic-vs-Connect mode is process-global; a parent Connect session crashes the
# imperative classic LocalSparkExecutor with CONNECT_URL_NOT_SET). These tests assert
# the two poison points are delegated to subprocess helpers, the guard fails loudly,
# and the imperative classic path is independent of Connect. We MUST NOT actually
# create an in-process Connect session here (it would poison the pytest process), so
# we mock the subprocess delegation and assert SparkSession.Builder.remote is never
# called in-process.
# ---------------------------------------------------------------------------
def _no_inprocess_connect():
    """Context manager: fail loudly if anything creates an in-process Connect
    session (SparkSession.builder.remote(...)) while it is active."""
    from pyspark.sql import SparkSession
    return mock.patch.object(
        SparkSession.Builder, "remote",
        side_effect=AssertionError("in-process Connect session created in the parent!"))


def test_ensure_schema_goes_through_subprocess_not_inprocess_session():
    srv = LocalConnectServer(port=15002, ui_port=4040, spark_home="/opt/spark")
    ok = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    with _no_inprocess_connect(), \
         mock.patch.object(lc, "_run_connect_helper", return_value=ok) as helper:
        srv.ensure_schema("spark_catalog", "default")
    helper.assert_called_once()
    argv = helper.call_args.args[0]
    assert argv[0] == "ensure-schema"
    assert "--remote" in argv and srv.remote in argv
    assert "--catalog" in argv and "spark_catalog" in argv
    assert "--database" in argv and "default" in argv


def test_ensure_schema_raises_if_subprocess_fails():
    srv = LocalConnectServer(port=15002, ui_port=4040, spark_home="/opt/spark")
    bad = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="boom")
    with _no_inprocess_connect(), mock.patch.object(lc, "_run_connect_helper", return_value=bad):
        raised = False
        try:
            srv.ensure_schema()
        except RuntimeError as e:
            raised = True
            assert "ensure_schema subprocess failed" in str(e)
        assert raised


def test_sdp_grading_builds_profile_via_subprocess_helper():
    """build_output_profile_subprocess shells to the helper (which owns the Connect
    session) and reconstructs the OutputProfile from its JSON result -- no in-process
    Connect session in the parent."""
    ex = LocalConnectExecutor("sc://localhost:15002/;user_id=alice", "http://localhost:4040")
    task_spec = {"output_contract": {"table": "agent_output", "revenue_col": "rev",
                                     "substrate": "orders"},
                 "defects_in_scope": ["D8"]}

    def fake_helper(argv_tail):
        # the helper writes the profile JSON to the --result path; emulate that.
        assert argv_tail[0] == "output-profile"
        result = argv_tail[argv_tail.index("--result") + 1]
        assert "--remote" in argv_tail and ex.spark_remote in argv_tail
        assert "--input" in argv_tail and "file:///data/x.ndjson" in argv_tail
        with open(result, "w") as f:
            json.dump({"d8_dollars_dropped": 42.5, "d8_rows_dropped": 1,
                       "reconciles": False, "extra": {"d8": {"shipped_total": 1.0}}}, f)
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    with _no_inprocess_connect(), mock.patch.object(lc, "_run_connect_helper", side_effect=fake_helper):
        prof = ex.build_output_profile_subprocess(task_spec, "file:///data/x.ndjson", "agent_output")
    assert prof is not None
    assert prof.d8_dollars_dropped == 42.5
    assert prof.d8_rows_dropped == 1
    assert prof.reconciles is False
    assert prof.extra.get("d8", {}).get("shipped_total") == 1.0


def test_sdp_grading_subprocess_failure_is_graceful():
    ex = LocalConnectExecutor("sc://localhost:15002", "http://localhost:4040")
    task_spec = {"output_contract": {"table": "agent_output"}, "defects_in_scope": ["D8"]}
    bad = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="connect refused")
    with _no_inprocess_connect(), mock.patch.object(lc, "_run_connect_helper", return_value=bad):
        prof = ex.build_output_profile_subprocess(task_spec, "file:///data/x.ndjson", "agent_output")
    # COMPLETED run we couldn't read back -> a profile carrying the error, not a crash.
    assert prof is not None
    assert "output_profile_subprocess_error" in prof.extra


def test_build_profile_routes_sdp_to_subprocess_and_never_touches_spark():
    """runner._build_profile must call build_output_profile_subprocess (when present)
    and NEVER evaluate executor.spark for the SDP-local executor."""
    calls = {}

    class FakeSDPExecutor:
        def build_output_profile_subprocess(self, task_spec, dataset, out_table):
            calls["sub"] = (dataset, out_table)
            return runner.oraclesmod.OutputProfile()

        @property
        def spark(self):
            raise AssertionError("runner touched executor.spark for an SDP-local cell!")

        def read_table(self, name):
            raise AssertionError("runner touched executor.read_table for an SDP-local cell!")

    ep = SimpleNamespace(completed=True, green_exec=SimpleNamespace(output_metrics=None))
    task_spec = {"output_contract": {"table": "agent_output"}, "defects_in_scope": ["D8"]}
    prof = runner._build_profile(ep, FakeSDPExecutor(), task_spec, "file:///data/x.ndjson")
    assert prof is not None
    assert calls["sub"] == ("file:///data/x.ndjson", "agent_output")


def test_build_profile_sdp_no_contract_returns_none_without_touching_spark():
    """REGRESSION (p5_mart crash): a task with NO output_contract must NOT make
    _build_profile fall through to `executor.spark` for an SDP-local executor whose
    `.spark` property RAISES RuntimeError (which getattr-with-default does not catch).
    It must dispatch on the subprocess-helper capability EXCLUSIVELY and return None."""
    touched = {"spark": 0, "read_table": 0, "sub": 0}

    class FakeSDPExecutor:
        # mirrors LocalConnectExecutor: the session accessors RAISE in the parent.
        def build_output_profile_subprocess(self, task_spec, dataset, out_table):
            touched["sub"] += 1
            return runner.oraclesmod.OutputProfile()

        @property
        def spark(self):
            touched["spark"] += 1
            raise RuntimeError("Refusing to create a Spark Connect session ... (parent)")

        def read_table(self, name):
            touched["read_table"] += 1
            raise RuntimeError("Refusing to create a Spark Connect session ... (parent)")

    ep = SimpleNamespace(completed=True, green_exec=SimpleNamespace(output_metrics=None))
    # p5_mart-style: completed run, defects in scope, but NO machine-readable contract.
    task_spec = {"id": "p5_mart", "defects_in_scope": ["D1", "D4"]}  # no output_contract
    prof = runner._build_profile(ep, FakeSDPExecutor(), task_spec, "file:///data/x.ndjson")
    assert prof is None, "no contract -> no profile"
    assert touched["spark"] == 0, "must NOT touch executor.spark (the p5_mart crash)"
    assert touched["read_table"] == 0
    # contract-less cells don't call the subprocess helper either (nothing to grade).
    assert touched["sub"] == 0


def test_p5_mart_style_cell_completes_with_real_row_not_harness_error():
    """End-to-end: a completed p5_mart-style SDP-local cell (no output_contract, whose
    executor.spark RAISES) must grade to a REAL completed row -- NOT a runner crash and
    NOT a harness_error. Structural defects (D1/D4) are graded from the logs; the
    missing output profile is legitimately None."""
    from harness.backends.base import LoopState
    from harness.backends.local import ScriptedBrain

    class FakeSDPExecutor:
        name = "local_connect"

        def __init__(self):
            self._materialized = False

        def reachable(self):
            return True

        def run_gate(self, proposal, arm, state):
            from harness.backends.base import GateOutcome
            return GateOutcome(failed=False, wall_s=0.1, log="gate: analyzed OK")

        def run_execute(self, proposal, arm, state):
            from harness.backends.base import ExecOutcome
            return ExecOutcome(failed=False, completed=True, wall_s=0.2,
                               executor_seconds=1.0, log="Run is COMPLETED",
                               output_tables=["customer_segments"])

        def build_output_profile_subprocess(self, task_spec, dataset, out_table):
            return None   # no contract -> nothing to grade

        @property
        def spark(self):
            raise RuntimeError("parent Connect session guard (must not be hit)")

        def advance(self):
            pass

        def stop(self):
            pass

    cfg = runner.StudyConfig.from_file(os.path.join(STUDY, "config", "study.config.json"))
    task_spec = {"id": "p5_mart", "defects_in_scope": ["D1", "D4"],
                 "oracles": {}, "graded_by": "output_oracle"}  # no output_contract
    ex = FakeSDPExecutor()
    brain = ScriptedBrain([{"code": "print('Run is COMPLETED')", "command": "spark-pipelines run"}])
    with tempfile.TemporaryDirectory() as tmp:
        row = runner.run_cell(task_spec, ARMS["B"], 0, cfg,
                              lambda *a, **k: brain, lambda *a, **k: ex, tmp, 1750000000.0)
    assert row.exit_class == "completed", row.exit_class
    assert row.task_success is True
    assert "harness_error" not in row.exit_class
    assert runner.validate_row(__import__("dataclasses").asdict(row)) == []


def test_local_connect_executor_spark_and_read_table_guard_in_parent():
    """The guard fails LOUDLY: any parent-process attempt to get a Connect session
    via the executor raises a clear RuntimeError pointing at the subprocess helper."""
    ex = LocalConnectExecutor("sc://localhost:15002", "http://localhost:4040")
    for getter in (lambda: ex.spark, lambda: ex.read_table("agent_output")):
        raised = False
        try:
            getter()
        except RuntimeError as e:
            raised = True
            assert "process-global" in str(e) and "subprocess helper" in str(e)
        assert raised, "the parent Connect-session guard must raise"


def test_imperative_classic_session_is_independent_of_connect():
    """An imperative cell's session is classic local[*] -- it uses
    SparkSession.builder.master(...), NEVER .remote(...), so it does not depend on (or
    poison, or get poisoned by) any Connect state."""
    chain = mock.MagicMock(name="builder")
    chain.master.return_value = chain
    chain.appName.return_value = chain
    chain.config.return_value = chain
    fake_session = mock.MagicMock(name="session")
    chain.getOrCreate.return_value = fake_session
    FakeSparkSession = mock.MagicMock()
    FakeSparkSession.builder = chain

    ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir="/tmp/wh", ui_port=4099)
    with mock.patch("pyspark.sql.SparkSession", FakeSparkSession):
        s = ex.spark
    assert s is fake_session
    chain.master.assert_called_once_with("local[2]")   # CLASSIC local mode
    chain.remote.assert_not_called()                   # NEVER Connect mode


# ---------------------------------------------------------------------------
# Execution timeout: a hung / streaming agent program must NEVER wedge the run.
# In-process imperative exec is bounded by a watchdog thread (stops active streams +
# cancels jobs on timeout); the ConnectExecutor command path is bounded by a
# subprocess timeout that kills the child process group. We exercise both with a
# SHORT timeout + a sleeping fixture (no real Spark: the session is mocked).
# ---------------------------------------------------------------------------
def _fake_classic_spark():
    sp = mock.MagicMock(name="classic_spark")
    sp.streams.active = []            # nothing to stop; _interrupt_spark stays a no-op
    return sp


def _fake_read_output_path(path):
    df = mock.MagicMock(name="path_df")
    df.limit.return_value.collect.return_value = []
    return df


def _write(ws, name, body):
    os.makedirs(ws, exist_ok=True)
    with open(os.path.join(ws, name), "w") as f:
        f.write(body)


def _loop_state(ws):
    from harness.backends.base import LoopState
    return LoopState(task="t", seed=0, workspace=ws, dataset_path="x",
                     output_table="agent_output")


def test_inprocess_agent_timeout_is_graceful_execution_timeout():
    """A hanging imperative agent program (sleep >> timeout) is killed by the watchdog
    and becomes a FAILED EXECUTION_TIMEOUT iteration -- fast, no hang, no crash."""
    from harness.backends.base import Proposal
    with tempfile.TemporaryDirectory() as tmp:
        _write(tmp, "pipeline.py", "import time\ntime.sleep(30)\n")
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir=tmp,
                                ui_port=4099, exec_timeout_s=1)
        ex._spark = _fake_classic_spark()   # avoid a real JVM in the unit test
        p = Proposal(iteration=0, code="x", command="python")
        t0 = time.time()
        # B2 withdrawn to arms/supplementary per paper §6.1 (2026-06-29); the imperative
        # run_gate watchdog is arm-agnostic, so active arm A exercises the same path.
        gate = ex.run_gate(p, ARMS["A"], _loop_state(tmp))
        out = ex.run_execute(p, ARMS["A"], _loop_state(tmp))
        elapsed = time.time() - t0
    assert gate.failed and gate.error_class == "EXECUTION_TIMEOUT", gate.error_class
    assert out.failed and not out.completed and out.error_class == "EXECUTION_TIMEOUT"
    assert elapsed < 20, f"watchdog did not bound execution (took {elapsed:.1f}s)"


def test_inprocess_normal_program_completes_within_timeout():
    """A fast-completing program runs to rc=0 through the watchdog and the runner's
    in-process read-back completion check passes (the watchdog does not break the
    normal path; real-Spark read-back is covered by test_live_path)."""
    from harness.backends.base import Proposal
    with tempfile.TemporaryDirectory() as tmp:
        _write(tmp, "pipeline.py", "print('Run is COMPLETED')\n")
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir=tmp,
                                ui_port=4099, exec_timeout_s=30)
        ex._spark = _fake_classic_spark()
        ex.read_output_path = _fake_read_output_path  # type: ignore[assignment]
        out = ex.run_execute(Proposal(0, "x", "python"), ARMS["A"], _loop_state(tmp))
    assert not out.failed and out.completed, (out.failed, out.error_class)


def test_run_episode_survives_hanging_agent():
    """End-to-end: a hanging agent does not wedge run_episode -- every iteration is a
    graceful EXECUTION_TIMEOUT failure, the loop proceeds, and the agent is fed the
    'do not start an unbounded streaming query' guidance."""
    from harness.backends.local import ScriptedBrain
    cfg = runner.StudyConfig.from_file(os.path.join(STUDY, "config", "study.config.json"))
    arm = SimpleNamespace(max_iterations=2, dry_run_gate=False,
                          paradigm="imperative_pyspark", arm_id="A")
    brain = ScriptedBrain([{"code": "import time\ntime.sleep(30)\n", "command": "python"}])
    with tempfile.TemporaryDirectory() as tmp:
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir=tmp,
                                ui_port=4099, exec_timeout_s=1)
        ex._spark = _fake_classic_spark()
        state = _loop_state(tmp)
        t0 = time.time()
        ep = runner.run_episode(brain, ex, arm, state, cfg)
        elapsed = time.time() - t0
    assert not ep.completed
    assert ep.exit_class == "max_iterations"
    assert len(ep.per_iteration) == 2
    for rec in ep.per_iteration:
        assert rec["execute"]["error_class"] == "EXECUTION_TIMEOUT"
    assert any("awaitTermination" in fb for fb in state.feedback), state.feedback
    assert elapsed < 20, f"run_episode wedged ({elapsed:.1f}s)"


def test_connect_run_command_timeout_kills_process_group():
    """ConnectExecutor._run bounds the gate/execute COMMAND: a sleeping child is killed
    (process group) and returns rc=124 / EXECUTION_TIMEOUT, fast."""
    from harness.backends.live import ConnectExecutor, extract_error_class
    ex = ConnectExecutor("sc://localhost:15002", None, cmd_timeout_s=1)
    with tempfile.TemporaryDirectory() as tmp:
        t0 = time.time()
        rc, log, wall = ex._run(["python3", "-c", "import time; time.sleep(30)"], tmp, {})
        elapsed = time.time() - t0
    assert rc == 124, rc
    assert extract_error_class(log) == "EXECUTION_TIMEOUT", log
    assert elapsed < 15, f"_run did not bound the command (took {elapsed:.1f}s)"


def test_connect_run_execute_imperative_timeout():
    """End-to-end ConnectExecutor.run_execute (imperative) on a hanging pipeline.py ->
    graceful EXECUTION_TIMEOUT failure, no hang."""
    from harness.backends.live import ConnectExecutor
    from harness.backends.base import Proposal
    ex = ConnectExecutor("sc://localhost:15002", None, cmd_timeout_s=1)
    with tempfile.TemporaryDirectory() as tmp:
        _write(tmp, "pipeline.py", "import time\ntime.sleep(30)\n")
        out = ex.run_execute(Proposal(0, "x", "python"), ARMS["A"], _loop_state(tmp))
    assert out.failed and not out.completed and out.error_class == "EXECUTION_TIMEOUT"


def test_failure_feedback_includes_timeout_guidance():
    fb = runner._failure_feedback("runtime", "EXECUTION_TIMEOUT")
    assert fb.startswith("[runtime] EXECUTION_TIMEOUT")
    assert "awaitTermination" in fb and "bounded input" in fb
    # a non-timeout class gets the bare code, no guidance.
    assert runner._failure_feedback("runtime", "UNRESOLVED_COLUMN") == "[runtime] UNRESOLVED_COLUMN"


def test_timeout_iteration_is_zero_cost_and_not_a_dry_run_intercept():
    """BLOCKING 1: a HARD-KILLED (EXECUTION_TIMEOUT) iteration must contribute $0,
    executor_seconds=None, executor_seconds_wallclock=0.0, and count as a failing
    iteration but NOT a dry-run intercept -- never the ~timeout wall-clock fallback
    pricing. Covered for both the execute path (no gate) and the gate path (B2-like)."""
    from harness.backends.local import ScriptedBrain
    cfg = runner.StudyConfig.from_file(os.path.join(STUDY, "config", "study.config.json"))
    brain_code = [{"code": "import time\ntime.sleep(30)\n", "command": "python"}]

    for gate in (False, True):
        arm = SimpleNamespace(max_iterations=2, dry_run_gate=gate,
                              paradigm="imperative_pyspark", arm_id="B2" if gate else "A")
        with tempfile.TemporaryDirectory() as tmp:
            ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir=tmp,
                                    ui_port=4099, exec_timeout_s=1)
            ex._spark = _fake_classic_spark()
            ep = runner.run_episode(ScriptedBrain(brain_code), ex, arm, _loop_state(tmp), cfg)
        rc = costmod_aggregate(ep)
        assert rc.total_usd == 0.0, (gate, rc.total_usd)
        assert rc.total_executor_seconds is None, (gate, rc.total_executor_seconds)
        assert rc.total_executor_seconds_wallclock == 0.0, (gate, rc.total_executor_seconds_wallclock)
        assert rc.failing_iterations == 2, (gate, rc.failing_iterations)
        assert rc.dry_run_intercepts == 0, (gate, rc.dry_run_intercepts)
        assert rc.intercept_fraction == 0.0, (gate, rc.intercept_fraction)


def test_interrupt_spark_stops_active_queries_and_cancels_jobs():
    """NON-BLOCKING strengthening: on timeout the watchdog must actually UNBLOCK a hung
    awaitTermination -- assert _interrupt_spark stops EACH active streaming query and
    cancels all jobs (not just that the watchdog returns)."""
    q1, q2 = mock.MagicMock(name="q1"), mock.MagicMock(name="q2")
    fake = mock.MagicMock(name="spark")
    fake.streams.active = [q1, q2]
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    ex._spark = fake
    ex._interrupt_spark()
    q1.stop.assert_called_once()
    q2.stop.assert_called_once()
    fake.sparkContext.cancelAllJobs.assert_called_once()


def test_late_worker_does_not_clobber_next_cell_argv_and_env():
    """BLOCKING 2: after a timeout the main thread moves on; a late-waking daemon
    worker's restore must be a complete NO-OP (sys.argv + env + streams), not reset to
    the previous cell's values. We time the fixture to wake the worker AFTER the call
    returns and assert the sentinel ('next cell') state survives."""
    from harness.backends.base import Proposal
    saved_argv = list(sys.argv)
    saved_env = {k: os.environ.get(k) for k in ("AGENT_INPUT_PATH", "AGENT_OUTPUT_PATH", "AGENT_OUTPUT_TABLE")}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            # the worker sleeps ~1.5s; the watchdog times out at 0.5s, so the worker's
            # `finally` restore fires ~1s AFTER the call returns -- the race window.
            _write(tmp, "pipeline.py", "import time\ntime.sleep(1.5)\n")
            ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir=tmp,
                                    ui_port=4099, exec_timeout_s=0.5)
            ex._spark = _fake_classic_spark()
            rc, log = ex._run_agent_program(_loop_state(tmp), analyze_only=True)
            assert rc == 124 and "EXECUTION_TIMEOUT" in log
            assert ex._exec_token is None, "token must be invalidated on timeout"
            # main thread proceeds to the 'next cell': install sentinel global state.
            sys.argv = ["__NEXT_CELL__"]
            os.environ["AGENT_INPUT_PATH"] = "__next_input__"
            os.environ["AGENT_OUTPUT_PATH"] = "__next_output__"
            os.environ.pop("AGENT_OUTPUT_TABLE", None)
            # wait past the worker's wake so its finally->_restore has run (and no-oped).
            time.sleep(2.0)
            assert sys.argv == ["__NEXT_CELL__"], f"stale worker clobbered argv: {sys.argv}"
            assert os.environ.get("AGENT_INPUT_PATH") == "__next_input__", \
                "stale worker clobbered env AGENT_INPUT_PATH"
            assert "AGENT_OUTPUT_TABLE" not in os.environ, \
                "stale worker re-set env AGENT_OUTPUT_TABLE"
    finally:
        sys.argv = saved_argv
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


# ---------------------------------------------------------------------------
# POST-EXECUTION hang: the live calibration wedge. The agent program returns rc=0
# and materializes output (_SUCCESS written), then an in-process py4j call AFTER the
# agent-exec timeout region -- the completion read-back, the SparkSession.stop, or the
# out-of-executor profile build -- hangs forever because the JVM wedged. AGENT_EXEC_
# TIMEOUT_S does not cover these steps. The fix bounds each in-executor step and adds a
# HARD per-cell wall-clock guard that abandons the cell and advances the sweep.
# ---------------------------------------------------------------------------
def _wedged_read_output_path(path):
    """A read-back whose .limit(0).collect() never returns (in-process JVM wedged)."""
    df = mock.MagicMock(name="wedged_df")
    df.limit.return_value.collect.side_effect = lambda *a, **k: time.sleep(10_000)
    return df


def test_post_exec_readback_hang_is_bounded_execution_timeout():
    """ROOT CAUSE: a post-_SUCCESS read-back that wedges (JVM stuck after materializing
    output) must NOT hang -- it is bounded by post_exec_timeout_s and fails fast as
    EXECUTION_TIMEOUT, not an indefinite stall."""
    from harness.backends.base import Proposal
    with tempfile.TemporaryDirectory() as tmp:
        ws = os.path.join(tmp, "ws"); os.makedirs(ws)
        outp = os.path.join(ws, "agent_output.parquet"); os.makedirs(outp)
        open(os.path.join(outp, "_SUCCESS"), "w").close()
        _write(ws, "pipeline.py", "print('Run is COMPLETED')\n")
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir=tmp,
                                ui_port=4099, exec_timeout_s=30, post_exec_timeout_s=1)
        ex._spark = _fake_classic_spark()
        ex.read_output_path = _wedged_read_output_path  # type: ignore[assignment]
        from harness.backends.base import LoopState
        st = LoopState(task="t", seed=0, workspace=ws, dataset_path="x",
                       output_table="agent_output", output_path=outp)
        t0 = time.time()
        # A2 withdrawn to arms/supplementary per paper §6.1 (2026-06-29); the imperative
        # run_execute is arm-agnostic here, so active imperative arm A exercises it.
        out = ex.run_execute(Proposal(0, "x", "python"), ARMS["A"], st)
        elapsed = time.time() - t0
    assert out.failed and not out.completed, (out.failed, out.completed)
    assert out.error_class == "EXECUTION_TIMEOUT", out.error_class
    assert elapsed < 10, f"post-exec read-back was not bounded (took {elapsed:.1f}s)"


def test_stop_hang_is_bounded_and_force_kills():
    """A SparkSession.stop() that wedges must be bounded (not hang the cell at teardown)
    and trigger a best-effort force-kill of the in-process JVM."""
    sp = mock.MagicMock(name="spark")
    sp.stop.side_effect = lambda *a, **k: time.sleep(10_000)
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099,
                            post_exec_timeout_s=1)
    ex._spark = sp
    with mock.patch("harness.backends.local._force_kill_local_jvm") as fk:
        t0 = time.time()
        ex.stop()
        elapsed = time.time() - t0
    assert elapsed < 10, f"stop() was not bounded (took {elapsed:.1f}s)"
    fk.assert_called_once()
    assert ex._spark is None


def test_abandoned_executor_teardown_is_noop():
    """An abandoned executor's stop()/_interrupt_spark must be COMPLETE no-ops: the
    per-cell guard already force-killed the JVM, so touching the dead py4j gateway (and
    triggering PySpark's class-global SparkContext.stop side effects) could tear down
    the NEXT cell's fresh session."""
    sp = mock.MagicMock(name="spark")
    sp.streams.active = [mock.MagicMock()]
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    ex._spark = sp
    ex._abandoned = True
    ex._interrupt_spark()
    sp.sparkContext.cancelAllJobs.assert_not_called()
    sp.streams.active[0].stop.assert_not_called()
    ex.stop()
    sp.stop.assert_not_called()


def test_abandon_active_local_executor_marks_and_force_kills():
    """abandon_active_local_executor flags the most-recently-created executor and
    force-kills the JVM (the per-cell guard's hook)."""
    from harness.backends import local as localmod
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    assert localmod._ACTIVE_LOCAL_EXECUTOR is ex
    with mock.patch("harness.backends.local._force_kill_local_jvm") as fk:
        abandoned = localmod.abandon_active_local_executor()
    assert abandoned is True
    assert ex._abandoned is True
    fk.assert_called_once()


# ---------------------------------------------------------------------------
# The HARD per-cell wall-clock guard: a wedged cell degrades to ONE bad row and the
# sweep advances. The hanging step is simulated by monkeypatching run_cell to sleep, so
# no real Spark/JVM is needed.
# (These use ARMS["A"] as a generic active-arm carrier; A2 was withdrawn to
# arms/supplementary per paper §6.1 (2026-06-29) and is no longer loaded from arms/.
# The guard is arm-agnostic, so the swap is behavior-preserving.)
# ---------------------------------------------------------------------------
_TASK_SPEC = {"id": "orders_silver_gold", "defects_in_scope": ["D8"],
              "output_contract": {"table": "agent_output"}}


def _cfg():
    return runner.StudyConfig.from_file(os.path.join(STUDY, "config", "study.config.json"))


def test_per_cell_guard_returns_real_row_for_normal_cell():
    """A cell that finishes within the deadline returns its REAL row unchanged."""
    sentinel = mock.Mock(name="real_row")
    with mock.patch.object(runner, "run_cell", return_value=sentinel) as rc:
        row = runner.run_cell_guarded(_TASK_SPEC, ARMS["A"], 0, _cfg(), None, None,
                                      "/tmp/work", 1750000000.0, per_cell_timeout_s=5)
    assert row is sentinel
    rc.assert_called_once()


def test_per_cell_guard_abandons_wedged_cell_with_bounded_timeout_row():
    """A cell whose run_cell WEDGES (sleeps past the deadline) is abandoned: a bounded
    harness_error timeout row is returned, the JVM is force-killed, and the call returns
    fast -- no infinite stall."""
    def _hang(*a, **k):
        time.sleep(10_000)

    with mock.patch.object(runner, "run_cell", side_effect=_hang), \
         mock.patch("harness.backends.local.abandon_active_local_executor") as ab:
        t0 = time.time()
        row = runner.run_cell_guarded(_TASK_SPEC, ARMS["A"], 0, _cfg(), None, None,
                                      "/tmp/work", 1750000000.0, per_cell_timeout_s=0.5,
                                      backend_name="local")
        elapsed = time.time() - t0
    assert elapsed < 5, f"guard did not bound the wedged cell (took {elapsed:.1f}s)"
    ab.assert_called_once()                     # the imperative JVM was force-killed
    assert row.exit_class == "harness_error", row.exit_class
    assert row.task_success is False and row.silent_defect is False
    assert row.run_id == "orders_silver_gold__A__seed0"
    assert "PER_CELL_TIMEOUT" in (row.notes or "")
    # the emitted row is schema-valid (the sweep can write it without warnings).
    assert runner.validate_row(__import__("dataclasses").asdict(row)) == []


def test_per_cell_guard_catches_residual_exception_as_safety_net():
    """SAFETY NET: a residual uncaught exception from run_cell (e.g. a profile/read-back
    crash) must NOT kill the whole sweep -- it degrades to a bounded harness_error
    CELL_ERROR row so the sweep advances. (The PRIMARY paths are expected not to raise;
    this is the last-resort net.)"""
    with mock.patch.object(runner, "run_cell", side_effect=ValueError("boom")):
        row = runner.run_cell_guarded(_TASK_SPEC, ARMS["A"], 0, _cfg(), None, None,
                                      "/tmp/work", 1750000000.0, per_cell_timeout_s=5,
                                      backend_name="local")
    # MERGE: run_cell_guarded now delegates classification to the unified per-cell net
    # (_run_cell_safe / #32), so a residual escaped exception is the unified HARNESS_EXCEPTION
    # crash-safety class (a HARNESS_FAULT, so the quarantine layer can also act on it),
    # NOT the legacy lowercase "harness_error". It is still a bounded soft-failed row.
    assert row.exit_class == "HARNESS_EXCEPTION", row.exit_class
    assert row.task_success is False and row.silent_defect is False
    assert "CELL_ERROR" in (row.notes or "") and "ValueError" in (row.notes or "")
    assert "boom" in (row.notes or "")
    assert runner.validate_row(__import__("dataclasses").asdict(row)) == []


def test_sweep_advances_past_a_wedged_cell():
    """End-to-end forward progress: cell #1 wedges, cell #2 is normal. The guard turns
    #1 into a timeout row and #2 still runs -- the sweep is NOT stalled by the bad cell.
    """
    real_row = mock.Mock(name="real_row")
    calls = {"n": 0}

    def _first_hangs_then_ok(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            time.sleep(10_000)        # cell #1 wedges
        return real_row               # cell #2 completes normally

    rows = []
    with mock.patch.object(runner, "run_cell", side_effect=_first_hangs_then_ok), \
         mock.patch("harness.backends.local.abandon_active_local_executor"):
        for seed in (0, 1):
            rows.append(runner.run_cell_guarded(
                _TASK_SPEC, ARMS["A"], seed, _cfg(), None, None, "/tmp/work",
                1750000000.0, per_cell_timeout_s=0.5, backend_name="local"))
    assert rows[0].exit_class == "harness_error", "wedged cell #1 must be a timeout row"
    assert rows[1] is real_row, "the sweep must advance to (and complete) cell #2"


# ---------------------------------------------------------------------------
# BLOCKING 1 (cross-review): force-kill must be COMPLETE/race-free -- the JVM must be
# CONFIRMED dead and its fixed UI port RELEASED before the next cell starts. These use
# a REAL spawned child bound to a real port (a JVM surrogate), so they prove the actual
# kill + port-release machinery (NOT a mocked _force_kill_local_jvm).
# ---------------------------------------------------------------------------
def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _spawn_port_holder(port):
    """A child that BINDS+listens on `port` in its OWN process group (mirrors how
    PySpark isolates the gateway JVM) and then sleeps. Returns the Popen."""
    code = (
        "import socket,time\n"
        f"s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)\n"
        f"s.bind(('127.0.0.1',{port}))\n"
        "s.listen(8)\n"
        "import sys; sys.stdout.write('UP'); sys.stdout.flush()\n"
        "time.sleep(300)\n"
    )
    return subprocess.Popen([sys.executable, "-c", code], start_new_session=True,
                            stdout=subprocess.PIPE)


def _wait_listening(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.settimeout(0.3)
        ok = c.connect_ex(("127.0.0.1", port)) == 0
        c.close()
        if ok:
            return True
        time.sleep(0.1)
    return False


def test_force_kill_machinery_kills_real_child_and_releases_port():
    """INTEGRATION: a real child holding a real port is killed by PROCESS GROUP,
    proc.wait() confirms exit, the port is confirmed released, and a fresh listener can
    then bind the SAME port. No mocking of the kill -- this is the real machinery."""
    from harness.backends import local as localmod
    port = _free_port()
    child = _spawn_port_holder(port)
    try:
        assert _wait_listening(port), "child never bound the port"
        localmod._kill_proc_tree(child, timeout_s=10)
        assert child.poll() is not None, "child not reaped after _kill_proc_tree"
        assert localmod._wait_port_released(port, timeout_s=10), "port not released"
        # a subsequent listener can claim the SAME (fixed) port -> truly free.
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.bind(("127.0.0.1", port))
        s2.listen(1)
        s2.close()
    finally:
        if child.poll() is None:
            child.kill()


def test_force_kill_local_jvm_kills_gateway_proc_resets_globals_and_frees_port():
    """INTEGRATION: _force_kill_local_jvm follows the REAL py4j gateway.proc handle to a
    live child, kills+reaps it, resets PySpark's class globals, confirms the UI port is
    released, and returns True."""
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from harness.backends import local as localmod

    port = _free_port()
    child = _spawn_port_holder(port)
    saved = {
        "sc": getattr(SparkContext, "_active_spark_context", None),
        "gw": getattr(SparkContext, "_gateway", None),
        "jvm": getattr(SparkContext, "_jvm", None),
        "inst": getattr(SparkSession, "_instantiatedSession", None),
        "act": getattr(SparkSession, "_activeSession", None),
    }
    try:
        assert _wait_listening(port), "child never bound the port"
        SparkContext._active_spark_context = SimpleNamespace(_gateway=SimpleNamespace(proc=child))
        ok = localmod._force_kill_local_jvm(ui_port=port, timeout_s=10)
        assert child.poll() is not None, "gateway JVM child not killed"
        assert ok is True, "force-kill did not confirm clean teardown"
        assert SparkContext._active_spark_context is None
        assert getattr(SparkContext, "_gateway", "x") is None
    finally:
        SparkContext._active_spark_context = saved["sc"]
        SparkContext._gateway = saved["gw"]
        SparkContext._jvm = saved["jvm"]
        SparkSession._instantiatedSession = saved["inst"]
        SparkSession._activeSession = saved["act"]
        if child.poll() is None:
            child.kill()


def test_discover_ui_port_parses_uiweburl():
    """H2 reads the ACTUAL bound UI port (Spark may fall back past the requested one)."""
    from harness.backends import local as localmod
    sp = SimpleNamespace(sparkContext=SimpleNamespace(uiWebUrl="http://host.example:4101"))
    assert localmod._discover_ui_port(sp) == 4101
    sp2 = SimpleNamespace(sparkContext=SimpleNamespace(uiWebUrl=None))
    assert localmod._discover_ui_port(sp2) is None


def test_executor_seconds_snapshot_uses_actual_bound_ui_port():
    """H2 executor-seconds REST must hit the DISCOVERED port, not the assumed ui_port,
    so contention-driven fallback can't make it read the wrong/absent app."""
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    ex._actual_ui_port = 4101            # Spark fell back past 4099
    seen = {}
    with mock.patch("harness.backends.local._get_json",
                    side_effect=lambda u: seen.setdefault("url", u) or []):
        ex._executor_seconds_snapshot()
    assert ":4101/" in seen["url"], seen.get("url")


# ---------------------------------------------------------------------------
# BLOCKING 3 (cross-review): a post-exec read-back timeout must force-kill+RESET the
# wedged JVM so the next iteration gets a FRESH session, not reuse the dead one.
# ---------------------------------------------------------------------------
def test_readback_timeout_force_kills_and_resets_session():
    """On read-back timeout, run_execute force-kills the wedged JVM AND drops the cached
    session (self._spark -> None) so the next iteration rebuilds fresh -- it does not
    leave the wedged session for the outer guard / final stop()."""
    from harness.backends.base import LoopState, Proposal
    with tempfile.TemporaryDirectory() as tmp:
        ws = os.path.join(tmp, "ws"); os.makedirs(ws)
        outp = os.path.join(ws, "agent_output.parquet"); os.makedirs(outp)
        open(os.path.join(outp, "_SUCCESS"), "w").close()
        _write(ws, "pipeline.py", "print('Run is COMPLETED')\n")
        ex = LocalSparkExecutor(out_table="agent_output", warehouse_dir=tmp,
                                ui_port=4099, exec_timeout_s=30, post_exec_timeout_s=1)
        ex._spark = _fake_classic_spark()
        ex.read_output_path = _wedged_read_output_path  # type: ignore[assignment]
        st = LoopState(task="t", seed=0, workspace=ws, dataset_path="x",
                       output_table="agent_output", output_path=outp)
        with mock.patch("harness.backends.local._force_kill_local_jvm",
                        return_value=True) as fk:
            out = ex.run_execute(Proposal(0, "x", "python"), ARMS["A"], st)
    assert out.error_class == "EXECUTION_TIMEOUT", out.error_class
    fk.assert_called_once()                       # force-kill happened immediately
    assert ex._spark is None, "wedged session must be reset so the next iter rebuilds"
    assert ex._actual_ui_port is None


def test_abandoned_executor_refuses_to_recreate_session():
    """An abandoned executor must NEVER spin up a new JVM (which would collide with the
    next cell on the fixed UI port): the .spark property raises instead of rebuilding."""
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    ex._abandoned = True
    ex._spark = None
    raised = False
    try:
        _ = ex.spark
    except RuntimeError as e:
        raised = True
        assert "abandoned" in str(e)
    assert raised, "abandoned executor must refuse to (re)create a session"


# ---------------------------------------------------------------------------
# BLOCKING 2 (cross-review): API/model failures must NOT be masked as ordinary agent
# no-code/max_iterations. Narrow handling: transient -> retried no-code (bounded);
# non-transient/persistent -> AgentApiError -> harness_error row; never swallow
# KeyboardInterrupt/SystemExit.
# ---------------------------------------------------------------------------
def _api_brain():
    # A2 withdrawn to arms/supplementary per paper §6.1 (2026-06-29); A is an active arm
    # carrying the identical base_model_id, so these brain/guard tests are unchanged.
    from harness.backends.live import AnthropicBrain
    return AnthropicBrain(ARMS["A"].base_model_id, "prompt", sampling={})


def _api_state(tmp):
    from harness.backends.base import LoopState
    return LoopState(task="orders_silver_gold", seed=0, workspace=tmp,
                     dataset_path="x", output_table="agent_output")


def test_propose_raises_agentapierror_on_non_transient_failure():
    """A non-transient API/client error (e.g. bad-request/auth/validation) surfaces as
    AgentApiError -- NOT a no-code agent turn."""
    from harness.backends.live import AgentApiError
    brain = _api_brain()
    fake = mock.MagicMock()
    fake.messages.create.side_effect = ValueError("400 invalid_request_error")
    brain._client = fake
    with tempfile.TemporaryDirectory() as tmp:
        raised = False
        try:
            brain.propose(_api_state(tmp), ARMS["A"])
        except AgentApiError as e:
            raised = True
            assert "API_ERROR" in str(e)
        assert raised, "non-transient API failure must raise AgentApiError"


def test_propose_escalates_persistent_transient_to_agentapierror():
    """A transient blip degrades to a retried no-code turn, but a PERSISTENT transient
    outage (>= TRANSIENT_API_FAILURE_LIMIT in a cell) escalates to AgentApiError so it
    is recorded as infra failure, not max_iterations attributed to the arm."""
    from harness.backends import live
    from harness.backends.live import AgentApiError
    brain = _api_brain()
    fake = mock.MagicMock()
    fake.messages.create.side_effect = TimeoutError("read timed out")
    brain._client = fake
    with tempfile.TemporaryDirectory() as tmp:
        st = _api_state(tmp)
        for i in range(live.TRANSIENT_API_FAILURE_LIMIT - 1):
            p = brain.propose(st, ARMS["A"])     # blips -> no-code, retried
            assert p.code == "" and p.stop_reason == "api_error", (i, p)
        raised = False
        try:
            brain.propose(st, ARMS["A"])         # the limit-th -> escalate
        except AgentApiError:
            raised = True
        assert raised, "persistent transient failure must escalate to AgentApiError"


def test_propose_does_not_swallow_keyboardinterrupt_or_systemexit():
    """propose must let KeyboardInterrupt/SystemExit propagate (never mask as no-code)."""
    for exc in (KeyboardInterrupt, SystemExit):
        brain = _api_brain()
        fake = mock.MagicMock()
        fake.messages.create.side_effect = exc()
        brain._client = fake
        with tempfile.TemporaryDirectory() as tmp:
            raised = False
            try:
                brain.propose(_api_state(tmp), ARMS["A"])
            except exc:
                raised = True
            assert raised, f"{exc.__name__} must propagate from propose"


def test_one_transient_failure_still_degrades_to_no_code():
    """Regression: a SINGLE transient timeout still degrades to a no-code retry turn
    (the loop gets another chance) -- only persistent failures escalate."""
    brain = _api_brain()
    fake = mock.MagicMock()
    fake.messages.create.side_effect = TimeoutError("blip")
    brain._client = fake
    with tempfile.TemporaryDirectory() as tmp:
        p = brain.propose(_api_state(tmp), ARMS["A"])
    assert p.code == "" and p.command == "" and p.stop_reason == "api_error"


def test_api_error_becomes_harness_error_row_not_max_iterations_or_success():
    """End-to-end: an AgentApiError out of a cell is recorded as a bounded harness_error
    row (distinguishable from a real arm outcome), NOT max_iterations and NOT
    task_success."""
    from harness.backends.live import AgentApiError
    with mock.patch.object(runner, "run_cell",
                           side_effect=AgentApiError("[API_ERROR] auth failed")):
        row = runner.run_cell_guarded(_TASK_SPEC, ARMS["A"], 0, _cfg(), None, None,
                                      "/tmp/work", 1750000000.0, per_cell_timeout_s=5,
                                      backend_name="local")
    # MERGE: the unified per-cell net classifies an escaped AgentApiError (no exit_class of
    # its own) as the HARNESS_EXCEPTION crash-safety class -- still a bounded harness-fault
    # row distinguishable from a real arm outcome, never max_iterations / task_success.
    assert row.exit_class == "HARNESS_EXCEPTION", row.exit_class
    assert row.exit_class != "max_iterations"
    assert row.task_success is False and row.silent_defect is False
    assert "AgentApiError" in (row.notes or "") and "API_ERROR" in (row.notes or "")
    assert runner.validate_row(__import__("dataclasses").asdict(row)) == []


def test_per_cell_guard_propagates_keyboardinterrupt():
    """The guard must NOT mask an operator interrupt as a row."""
    with mock.patch.object(runner, "run_cell", side_effect=KeyboardInterrupt()):
        raised = False
        try:
            runner.run_cell_guarded(_TASK_SPEC, ARMS["A"], 0, _cfg(), None, None,
                                    "/tmp/work", 1750000000.0, per_cell_timeout_s=5)
        except KeyboardInterrupt:
            raised = True
        assert raised, "KeyboardInterrupt must propagate through the guard"


# ---------------------------------------------------------------------------
# STALE SparkContext between consecutive imperative cells (the consecutive-A2
# harness_error). A CLEAN sp.stop() left PySpark's process-global singletons alive, so
# the NEXT local cell's getOrCreate() raised "Only one SparkContext should be running
# in this JVM" -> the cell crashed instantly (harness_error, iterations=0). The fix:
# stop() ALWAYS force-resets (not only on timeout), plus a defensive guard in .spark.
# ---------------------------------------------------------------------------
def test_clean_stop_always_force_resets_global_context():
    """PRIMARY (unit): a CLEAN, non-timed-out sp.stop() must STILL force-reset, so no
    stale classic SparkContext singleton can survive into the next cell. Before the fix
    the force-reset ran ONLY on the timeout path; a clean stop left the JVM globals
    alive and the next getOrCreate() raised 'Only one SparkContext ...'."""
    sp = mock.MagicMock(name="spark")           # sp.stop() returns immediately (clean)
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    ex._spark = sp
    ex._actual_ui_port = 4099
    with mock.patch("harness.backends.local._force_kill_local_jvm") as fk:
        ex.stop()
    sp.stop.assert_called_once()                 # the bounded clean stop still happened
    fk.assert_called_once()                      # AND the force-reset ran on the clean path
    assert ex._spark is None
    # the force-reset targets the ACTUAL bound UI port so the next cell's port is freed.
    called_port = fk.call_args.kwargs.get("ui_port")
    if called_port is None and fk.call_args.args:
        called_port = fk.call_args.args[0]
    assert called_port == 4099, called_port


def test_stop_force_resets_even_when_sp_stop_raises():
    """BLOCKING (cross-review): _run_bounded RE-RAISES a Py4J/Spark error from sp.stop(),
    so the force-reset MUST be in a finally -- a RAISED stop() must STILL force-kill+reset
    exactly once (and drop self._spark), or the global SparkContext/SparkSession
    singletons leak into the next cell. Covers the third path (clean / timeout / RAISED)."""
    sp = mock.MagicMock(name="spark")
    sp.stop.side_effect = RuntimeError("py4j: gateway died during stop()")
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    ex._spark = sp
    ex._actual_ui_port = 4099
    with mock.patch("harness.backends.local._force_kill_local_jvm") as fk:
        ex.stop()                                   # the re-raised error must NOT escape stop()
    sp.stop.assert_called_once()
    fk.assert_called_once()                          # force-reset STILL ran on the raised path
    assert ex._spark is None
    assert ex._actual_ui_port is None
    called_port = fk.call_args.kwargs.get("ui_port")
    if called_port is None and fk.call_args.args:
        called_port = fk.call_args.args[0]
    assert called_port == 4099, called_port


def test_spark_property_force_kills_stale_classic_context_before_getorcreate():
    """DEFENSIVE guard: if a stale classic SparkContext is still active when the
    executor builds its session, .spark must force-kill+reset it BEFORE getOrCreate()
    (which would otherwise raise 'Only one SparkContext should be running in this
    JVM'). Pure unit test: pyspark is faked so no real JVM is touched."""
    # The fake getOrCreate ASSERTS the reap already ran BEFORE it -- so a (broken)
    # implementation that reaped AFTER getOrCreate() would fail here, not just on the
    # call-count check (which a wrong-order impl could still satisfy).
    order = {"forced_before_getorcreate": None}

    class FakeBuilder:
        def __init__(self, fk):
            self._fk = fk
        def master(self, v): return self
        def appName(self, v): return self
        def config(self, k, v): return self
        def getOrCreate(self):
            order["forced_before_getorcreate"] = self._fk.called
            s = mock.Mock(); s.sparkContext = mock.Mock(); return s

    FakeSC = SimpleNamespace(_active_spark_context=object())  # a stale context is present
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    with mock.patch("harness.backends.local._force_kill_local_jvm") as fk:
        fk.return_value = True
        FakeSS = SimpleNamespace(builder=FakeBuilder(fk),
                                 getActiveSession=mock.Mock(return_value=None))
        with mock.patch.dict(sys.modules, {
                "pyspark": SimpleNamespace(SparkContext=FakeSC),
                "pyspark.sql": SimpleNamespace(SparkSession=FakeSS)}):
            _ = ex.spark
    fk.assert_called_once()                       # the stale context was reaped...
    assert order["forced_before_getorcreate"] is True, \
        "force-kill must run BEFORE getOrCreate(), not after"   # ...and BEFORE getOrCreate
    called_port = fk.call_args.kwargs.get("ui_port")
    if called_port is None and fk.call_args.args:
        called_port = fk.call_args.args[0]
    assert called_port == 4099, called_port


def test_spark_property_no_force_kill_when_no_stale_context():
    """Symmetry: with NO active classic context, the guard does NOT force-kill -- the
    common clean path stays a single getOrCreate(), no spurious JVM reap."""
    class FakeBuilder:
        def master(self, v): return self
        def appName(self, v): return self
        def config(self, k, v): return self
        def getOrCreate(self):
            s = mock.Mock(); s.sparkContext = mock.Mock(); return s

    FakeSC = SimpleNamespace(_active_spark_context=None)       # nothing to reap
    FakeSS = SimpleNamespace(builder=FakeBuilder(),
                             getActiveSession=mock.Mock(return_value=None))
    ex = LocalSparkExecutor(out_table="t", warehouse_dir="/tmp/wh", ui_port=4099)
    with mock.patch.dict(sys.modules, {
            "pyspark": SimpleNamespace(SparkContext=FakeSC),
            "pyspark.sql": SimpleNamespace(SparkSession=FakeSS)}), \
         mock.patch("harness.backends.local._force_kill_local_jvm") as fk:
        _ = ex.spark
    fk.assert_not_called()


def test_two_consecutive_local_sessions_no_only_one_sparkcontext():
    """PRIMARY (integration, real Spark): create a LocalSparkExecutor, run+teardown a
    trivial CLASSIC Spark session, then create a SECOND one in the SAME process. The
    second getOrCreate() must succeed with NO 'Only one SparkContext should be running
    in this JVM' -- the exact consecutive-A2 crash. Skips if pyspark/JDK absent."""
    try:
        import pyspark  # noqa: F401
        from pyspark import SparkContext
    except Exception:
        print("SKIP test_two_consecutive_local_sessions_no_only_one_sparkcontext: "
              "pyspark not importable")
        return
    with tempfile.TemporaryDirectory() as tmp:
        port = _free_port()
        ex1 = LocalSparkExecutor(out_table="t", warehouse_dir=os.path.join(tmp, "wh"),
                                 ui_port=port)
        try:
            assert ex1.spark.range(5).count() == 5      # real classic JVM #1
        finally:
            ex1.stop()
        # The fix guarantees a clean stop() leaves NO stale classic SparkContext behind.
        assert SparkContext._active_spark_context is None, \
            "stale classic SparkContext survived a clean stop() -> next cell would crash"

        ex2 = LocalSparkExecutor(out_table="t", warehouse_dir=os.path.join(tmp, "wh"),
                                 ui_port=port)
        try:
            # Before the fix this raised SparkException: Only one SparkContext ...
            assert ex2.spark.range(7).count() == 7      # real classic JVM #2, same process
        finally:
            ex2.stop()
        assert SparkContext._active_spark_context is None


def test_harness_error_row_carries_structured_error_field():
    """OBSERVABILITY: a crashed cell's harness_error row preserves the exception
    type+message in the structured `error` field (not ONLY truncated into `notes`),
    and stays schema-valid against both the validator and the published contract."""
    boom = RuntimeError("Only one SparkContext should be running in this JVM (app-x)")
    with mock.patch.object(runner, "run_cell", side_effect=boom):
        row = runner.run_cell_guarded(_TASK_SPEC, ARMS["A"], 0, _cfg(), None, None,
                                      "/tmp/work", 1750000000.0, per_cell_timeout_s=5,
                                      backend_name="local")
    # MERGE: escaped crash now carries the unified HARNESS_EXCEPTION crash-safety class
    # (#32); the structured-`error` observability (#42) is preserved end-to-end.
    assert row.exit_class == "HARNESS_EXCEPTION", row.exit_class
    # the structured field carries BOTH the exception type and the full message.
    assert row.error, "crashed cell row must populate the structured `error` field"
    assert "RuntimeError" in row.error and "Only one SparkContext" in row.error, row.error
    # notes is still kept (backward-compatible), so nothing is lost.
    assert "CELL_ERROR" in (row.notes or "")
    d = __import__("dataclasses").asdict(row)
    assert runner.validate_row(d) == [], runner.validate_row(d)
    import json as _json
    from harness.schema import RESULTS_JSON_SCHEMA
    import jsonschema as _js
    _js.validate(_json.loads(row.to_json()), RESULTS_JSON_SCHEMA)   # published contract


def test_harness_error_field_defaults_empty_for_non_crash_rows():
    """Backward-compat: the PER_CELL_TIMEOUT path has no exception, so `error` stays the
    empty default (an optional field) -- existing rows/readers are unaffected."""
    def _hang(*a, **k):
        time.sleep(10_000)
    with mock.patch.object(runner, "run_cell", side_effect=_hang), \
         mock.patch("harness.backends.local.abandon_active_local_executor"):
        row = runner.run_cell_guarded(_TASK_SPEC, ARMS["A"], 0, _cfg(), None, None,
                                      "/tmp/work", 1750000000.0, per_cell_timeout_s=0.5,
                                      backend_name="local")
    assert row.exit_class == "harness_error"
    assert row.error == "", repr(row.error)        # no exception -> empty structured field
    assert "PER_CELL_TIMEOUT" in (row.notes or "")


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
            import traceback
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
