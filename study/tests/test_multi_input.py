"""Multi-input staging guard (corpus v3 multi-input tasks), no network needed.

Corpus v3 added tasks that need MORE THAN ONE input: the stream-stream temporal
join + as-of SCD2 join (payments + an FX-rate feed) and the hard-mode HC-1
(trades + fx_rates) / HC-2 (clicks + users CDC). Each declares its extra
generators in `aux_inputs`. The runner previously staged only the PRIMARY input,
so the aux inputs never reached the agent. These tests pin the multi-input
contract:

  (1) every declared aux input is GENERATED at the seed and STAGED the same way
      the primary is (deterministic, identical across arms);
  (2) the staged aux map is threaded into LoopState.aux_inputs and exposed to the
      agent on BOTH paradigms -- imperative via AGENT_AUX_INPUTS env (+ per-name
      vars), SDP via the dataset-locations in the user message;
  (3) a task that declares aux_inputs actually gets them staged (the gating guard);
  (4) single-input tasks are byte-for-byte unchanged (empty aux env / map);
  (5) the added prompt wiring leaks no solution -- only locations + neutral names.
"""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import cost as costmod                   # noqa: E402
from harness import runner                            # noqa: E402
from harness.arm_manifest import ArmManifest, load_arms  # noqa: E402
from harness.backends.base import (ExecOutcome, GateOutcome, LoopState,  # noqa: E402
                                   Proposal, aux_input_env, aux_locations_text)
from harness.backends.live import AnthropicBrain, ConnectExecutor  # noqa: E402
from harness.backends.local import LocalSparkExecutor  # noqa: E402
from harness.prompt_guard import leaks as _leaks       # noqa: E402

ARMS = load_arms(os.path.join(STUDY, "arms"))
TASKS = json.load(open(os.path.join(STUDY, "config", "TASKS.lock.json")))
TASKS_BY_ID = {t["id"]: t for t in TASKS["tasks"]}
# tasks that declare at least one .py aux generator
AUX_TASKS = [t for t in TASKS["tasks"]
             if any(str(a).endswith(".py") for a in (t.get("aux_inputs") or []))]


def _cfg():
    return runner.StudyConfig(
        base_model_id="claude-opus-4-8",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(4, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://x:1/", spark_rest_url=None)


# ---------------------------------------------------------------------------
# (0) the corpus actually exercises multi-input (don't silently test nothing)
# ---------------------------------------------------------------------------
def test_corpus_declares_multi_input_tasks():
    ids = {t["id"] for t in AUX_TASKS}
    # the four v3 multi-input tasks named in the corpus upgrade
    assert {"new_stream_stream_join", "new_scd2_as_of_join",
            "HC1_fx_trade_ledger", "HC2_session_funnel"} <= ids, ids


def test_aux_input_name_strips_gen_prefix():
    assert runner.aux_input_name("generators/gen_fx_rates_cdc.py") == "fx_rates_cdc"
    assert runner.aux_input_name("generators/gen_customers_cdc.py") == "customers_cdc"


# ---------------------------------------------------------------------------
# (1)+(3) every declared aux input is generated AND staged (the gating guard)
# ---------------------------------------------------------------------------
def test_every_aux_task_generates_all_declared_inputs():
    cfg = _cfg()
    for t in AUX_TASKS:
        declared = [a for a in t["aux_inputs"] if str(a).endswith(".py")]
        with tempfile.TemporaryDirectory() as tmp:
            aux = runner.generate_aux_datasets(t, cfg, seed=42, out_dir=tmp)
            # the GATING GUARD: a task that declares N .py aux inputs gets N staged
            assert len(aux) == len(declared), \
                f"task {t['id']} declares {len(declared)} aux but generated {len(aux)}"
            for path in aux.values():
                assert os.path.exists(path) and os.path.getsize(path) > 0, \
                    f"task {t['id']} aux {path} empty/missing"
            # names are the gen_-stripped stems, keyed deterministically
            assert set(aux) == {runner.aux_input_name(a) for a in declared}


def test_single_input_tasks_get_no_aux():
    cfg = _cfg()
    single = [t for t in TASKS["tasks"] if not (t.get("aux_inputs") or [])]
    assert single, "expected some single-input tasks"
    with tempfile.TemporaryDirectory() as tmp:
        for t in single[:3]:
            assert runner.generate_aux_datasets(t, cfg, 42, tmp) == {}


def test_aux_generation_is_seed_deterministic():
    t = TASKS_BY_ID["new_stream_stream_join"]
    cfg = _cfg()
    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        ax = runner.generate_aux_datasets(t, cfg, 7, a)
        bx = runner.generate_aux_datasets(t, cfg, 7, b)
        assert set(ax) == set(bx)
        for name in ax:
            assert open(ax[name]).read() == open(bx[name]).read(), \
                f"aux {name} differs across two seed-7 generations (non-deterministic)"


# ---------------------------------------------------------------------------
# (2) staged aux is threaded into LoopState + exposed to BOTH paradigms via run_cell
# ---------------------------------------------------------------------------
class _RecordingExecutor:
    """A minimal executor that records every stage_input call and the LoopState the
    brain saw, then completes immediately. No Spark / cluster -- the OUTPUT-profile
    path falls back to canned metrics (None), which is fine: we test STAGING."""

    def __init__(self, name):
        self.name = name
        self.staged = []        # (local_path, subkey)

    def stage_input(self, local_path, subkey=None):
        self.staged.append((local_path, subkey))
        return f"staged://{subkey or 'primary'}"

    def run_gate(self, proposal, arm, state):
        return GateOutcome(failed=False, wall_s=0.01, log="gate ok")

    def run_execute(self, proposal, arm, state):
        return ExecOutcome(failed=False, completed=True, wall_s=0.01, log="Run is COMPLETED")

    def reachable(self):
        return True


def _run_cell_capturing(task_spec, arm):
    seen = {}
    rec_exec = _RecordingExecutor("rec_" + arm.arm_id)

    def make_brain(task, a, seed):
        class _B:
            name = "capture"
            def propose(self, state, arm_inner):
                seen["aux_inputs"] = dict(state.aux_inputs)
                seen["dataset_path"] = state.dataset_path
                return Proposal(iteration=0, code="x = 1\n", command="python")
        return _B()

    def make_executor(task, a, seed):
        return rec_exec

    with tempfile.TemporaryDirectory() as tmp:
        runner.run_cell(task_spec, arm, 42, _cfg(), make_brain, make_executor,
                        work_dir=tmp, clock=1750000000.0)
    return seen, rec_exec


def test_run_cell_stages_all_inputs_on_both_paradigms():
    t = TASKS_BY_ID["new_stream_stream_join"]   # payments + fx_rates_cdc
    declared = [runner.aux_input_name(a) for a in t["aux_inputs"] if str(a).endswith(".py")]
    for arm in (ARMS["A"], ARMS["B1"]):   # imperative (no gate) + SDP (no gate)
        seen, ex = _run_cell_capturing(t, arm)
        # PRIMARY staged with subkey=None; EACH aux staged under its own subkey
        subkeys = [sk for (_local, sk) in ex.staged]
        assert None in subkeys, f"arm {arm.arm_id}: primary input was not staged"
        for name in declared:
            assert name in subkeys, f"arm {arm.arm_id}: aux {name!r} not staged (subkeys={subkeys})"
        # the brain SAW the staged aux map threaded through LoopState
        assert set(seen["aux_inputs"]) == set(declared), \
            f"arm {arm.arm_id}: LoopState.aux_inputs = {seen['aux_inputs']}"
        for name in declared:
            assert seen["aux_inputs"][name] == f"staged://{name}"


# ---------------------------------------------------------------------------
# (2-imperative) AGENT_AUX_INPUTS env contract (present when aux, empty otherwise)
# ---------------------------------------------------------------------------
def _state_with_aux():
    return LoopState(task="t", seed=42, workspace="/ws", dataset_path="staged://primary",
                     aux_inputs={"fx_rates_cdc": "staged://fx_rates_cdc"},
                     output_table="gold_daily")


def test_aux_input_env_present_and_individual_vars():
    env = aux_input_env(_state_with_aux())
    assert env["AGENT_AUX_INPUTS"] == json.dumps({"fx_rates_cdc": "staged://fx_rates_cdc"},
                                                 sort_keys=True)
    assert env["AGENT_AUX_INPUT_FX_RATES_CDC"] == "staged://fx_rates_cdc"


def test_aux_input_env_empty_for_single_input():
    st = LoopState(task="t", seed=42, workspace="/ws", dataset_path="staged://primary")
    assert aux_input_env(st) == {}


def test_connect_imperative_env_includes_aux_when_present_else_unchanged():
    ex = ConnectExecutor("sc://x:1/", None)
    # single-input: byte-for-byte the original neutral env (regression lock)
    single = LoopState(task="t", seed=42, workspace="/ws", dataset_path="s3a://b/staged",
                       output_table="gold_daily")
    assert ex._imperative_env(single) == {
        "AGENT_INPUT_PATH": "s3a://b/staged", "AGENT_OUTPUT_TABLE": "gold_daily"}
    # multi-input: primary env PLUS the aux contract
    env = ex._imperative_env(_state_with_aux())
    assert env["AGENT_INPUT_PATH"] == "staged://primary"
    assert env["AGENT_OUTPUT_TABLE"] == "gold_daily"
    assert "fx_rates_cdc" in env["AGENT_AUX_INPUTS"]
    assert env["AGENT_AUX_INPUT_FX_RATES_CDC"] == "staged://fx_rates_cdc"


# ---------------------------------------------------------------------------
# (2-sdp) the SDP/imperative agent is TOLD each input location in the user message
# ---------------------------------------------------------------------------
def test_user_message_lists_aux_paths_both_paradigms():
    brain = AnthropicBrain("claude-opus-4-8", "TASK PROMPT")
    # IDENTICAL aux block for both paradigms (symmetry): same name + same location.
    msgs = [brain._user_message(_state_with_aux(), ARMS[a]) for a in ("B1", "A")]
    for arm_id, msg in zip(("B1", "A"), msgs):
        assert "staged://fx_rates_cdc" in msg, f"arm {arm_id}: aux path not in user message"
        assert "fx_rates_cdc" in msg
    # the aux block is byte-identical across paradigms (no env-var-only imperative path)
    block = aux_locations_text(_state_with_aux().aux_inputs)
    assert block and all(block in m for m in msgs)


def test_system_prompt_does_not_document_aux_env_for_either_paradigm():
    """Symmetry guard: the agent-visible SYSTEM prompt must NOT carry an
    imperative-only AGENT_AUX_INPUTS env contract (that was the BLOCKER-1 asymmetry).
    Aux is delivered identically to both paradigms via the location block."""
    brain = AnthropicBrain("claude-opus-4-8", "TASK PROMPT", omnigent_skills_dir=None)
    for aid in ("A", "B1"):   # imperative + sdp
        assert "AGENT_AUX_INPUTS" not in brain._system_prompt(ARMS[aid]), aid


# ---------------------------------------------------------------------------
# (2-imperative end-to-end) local classic executor injects + restores aux env
# ---------------------------------------------------------------------------
_AUX_ECHO_PROGRAM = """
import os, json
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
aux = os.environ.get("AGENT_AUX_INPUTS", "MISSING")
one = os.environ.get("AGENT_AUX_INPUT_FX_RATES_CDC", "MISSING")
spark.createDataFrame([(aux, one)], "aux string, one string") \\
     .write.mode("overwrite").parquet(os.environ["AGENT_OUTPUT_PATH"])
print("Run is COMPLETED")
"""


def test_local_imperative_program_receives_and_restores_aux_env():
    try:
        import pyspark  # noqa: F401
    except Exception:
        print("SKIP test_local_imperative_program_receives_and_restores_aux_env: no pyspark")
        return
    os.environ.pop("AGENT_AUX_INPUTS", None)
    os.environ.pop("AGENT_AUX_INPUT_FX_RATES_CDC", None)
    with tempfile.TemporaryDirectory() as tmp:
        ws = os.path.join(tmp, "cell")
        os.makedirs(ws)
        with open(os.path.join(ws, "pipeline.py"), "w") as f:
            f.write(_AUX_ECHO_PROGRAM)
        out_path = os.path.join(ws, "out.parquet")
        st = LoopState(task="t", seed=42, workspace=ws, dataset_path="file:///dev/null",
                       aux_inputs={"fx_rates_cdc": "file:///tmp/fx.ndjson"},
                       output_table="gold", output_path=out_path)
        ex = LocalSparkExecutor(out_table="gold", warehouse_dir=os.path.join(tmp, "wh"),
                                ui_port=4071)
        try:
            outcome = ex.run_execute(Proposal(0, _AUX_ECHO_PROGRAM, "python"), ARMS["A"], st)
            assert outcome.completed, f"agent program did not complete: {outcome.log}"
            row = ex.spark.read.parquet(out_path).collect()[0]
            assert json.loads(row["aux"]) == {"fx_rates_cdc": "file:///tmp/fx.ndjson"}, row["aux"]
            assert row["one"] == "file:///tmp/fx.ndjson", row["one"]
        finally:
            ex.stop()
    # RESTORED: the aux vars must not leak into this (or a later cell's) process env
    assert "AGENT_AUX_INPUTS" not in os.environ
    assert "AGENT_AUX_INPUT_FX_RATES_CDC" not in os.environ


# ---------------------------------------------------------------------------
# (5) the aux wiring is STRICTLY location-only + paradigm-symmetric, and the
#     leak checker -- run over the FULL agent-visible system+user text for BOTH
#     paradigms -- catches a 'read as NDJSON'-style how-to leak.
# ---------------------------------------------------------------------------
def test_aux_locations_text_is_location_only_and_symmetric():
    """The agent-visible aux block reveals ONLY name + location: no format label, no
    how-to verb, no env-var documentation. It is built once and used identically for
    both paradigms (symmetry by construction)."""
    block = aux_locations_text({"fx_rates_cdc": "file:///d/fx", "users_cdc": "file:///d/u"})
    assert block == ("Additional input locations:\n"
                     "  - fx_rates_cdc: file:///d/fx\n"
                     "  - users_cdc: file:///d/u")
    low = block.lower()
    for forbidden in ("ndjson", "read each", "read as", "agent_aux_inputs", "json", "parse"):
        assert forbidden not in low, f"aux block leaks how-to/format token {forbidden!r}"
    assert aux_locations_text({}) == ""


def _no_skill_arm(arm_id, paradigm, allowed):
    """A minimal arm with skills=[] so the composed system prompt carries ONLY the
    harness wiring under test -- NOT the linked-skill text, which is a separate arm
    treatment that DELIBERATELY contains technique vocabulary (graded elsewhere by
    test_skill_loading)."""
    return ArmManifest.from_dict({
        "arm_id": arm_id, "base_model_id": "claude-opus-4-8",
        "task_prompt_ref": "prompts/task_prompt.md@v1", "max_iterations": 12,
        "temperature": 0.0, "top_p": 1.0, "paradigm": paradigm, "dry_run_gate": True,
        "safety_skill": False, "skills": [], "allowed_commands": allowed, "description": ""})


def _full_agent_text(arm, state):
    """The FULL agent-visible text the harness composes: system prompt + user message
    (skills excluded via skills=[], output baked as a table contract via output_path='')."""
    brain = AnthropicBrain("claude-opus-4-8",
                           "Stakeholder ticket: the daily totals look off; produce usd_daily.",
                           omnigent_skills_dir=None)
    return brain._system_prompt(arm) + "\n" + brain._user_message(state, arm)


def test_full_agent_message_no_leak_both_paradigms():
    state = LoopState(task="new_stream_stream_join", seed=42, workspace="/ws",
                      dataset_path="file:///d/pay",
                      aux_inputs={"fx_rates_cdc": "file:///d/fx"},
                      output_table="usd_daily", output_path="")
    imp = _no_skill_arm("IMP", "imperative_pyspark",
                        ["python --analyze-only", "python", "spark-submit"])
    sdp = _no_skill_arm("SDP", "sdp", ["spark-pipelines dry-run", "spark-pipelines run"])
    for arm in (imp, sdp):
        full = _full_agent_text(arm, state)
        # the aux location is present (the agent IS told where the extra input is)...
        assert "fx_rates_cdc" in full and "file:///d/fx" in full, arm.arm_id
        # ...but the FULL composed text leaks no how-to / API / fix / format vocab.
        assert not _leaks(full), f"arm {arm.arm_id}: full agent text leaks {_leaks(full)}"


def test_leak_checker_catches_read_as_ndjson_regression():
    """Guard the guard: if someone re-adds a 'read each as NDJSON'-style how-to hint
    to the aux wiring, the checker MUST flag it (this is what makes BLOCKER-1 a
    regression test, not just a one-time fix)."""
    leaky = ("Additional input locations:\n  - fx_rates_cdc: file:///d/fx\n"
             "Read each as NDJSON, parsing the feed as you do the primary dataset.")
    hits = _leaks(leaky)
    assert hits, "leak checker failed to catch a 'read each as NDJSON' how-to leak"
    assert any("read" in h for h in hits), hits


def test_compose_prompt_announces_aux_without_leak():
    preamble = open(os.path.join(STUDY, "prompts", "task_prompt.md")).read()
    for t in AUX_TASKS:
        composed = runner.compose_task_prompt(preamble, t)
        assert "Additional inputs" in composed, f"task {t['id']} composed prompt omits aux section"
        for a in t["aux_inputs"]:
            if str(a).endswith(".py"):
                assert runner.aux_input_name(a) in composed
        assert not _leaks(composed), f"task {t['id']} aux prompt wiring leaks banned tokens"


def test_single_input_compose_prompt_has_no_aux_section():
    preamble = open(os.path.join(STUDY, "prompts", "task_prompt.md")).read()
    t = TASKS_BY_ID["p1_medallion"]   # single input
    assert "Additional inputs" not in runner.compose_task_prompt(preamble, t)


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback
            failed += 1; print(f"ERROR {t.__name__}: {type(e).__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
