"""The PUBLISHED results_schema.json must stay in lockstep with the runtime schema,
and EMITTED rows must validate against the PUBLISHED file -- including the UNMEASURED
GATED row the H2 stage-diff change can produce.

This is the drift guard that the compute change needed: `harness/schema.py` made
`executor_seconds` nullable and added the cpu/wall-clock compute fields, and the
runner emits them, but the checked-in machine-readable `results_schema.json`
(`additionalProperties: false`) was stale -- so a real emitted row (esp. an
unmeasured gated row: `executor_seconds: null` plus the new fields) passed
`validate_row()` in code yet would FAIL the published contract, breaking validation
of the real sweep. These tests fail if the file and the code disagree again.
"""
import dataclasses
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)
sys.path.insert(0, HERE)   # to reuse the stubs from test_stage_compute

import jsonschema                                       # noqa: E402

from harness import cost as costmod                     # noqa: E402
from harness import runner                              # noqa: E402
from harness.arm_manifest import load_arms               # noqa: E402
from harness.backends.base import GateOutcome            # noqa: E402
from harness.backends.local import ScriptedBrain         # noqa: E402
from harness.schema import RESULTS_JSON_SCHEMA, ResultRow, validate_row  # noqa: E402

# reuse the mocked-REST fixtures + stub executor from the stage-compute tests
from test_stage_compute import (  # noqa: E402
    EXPECT_CPU_S, EXPECT_EXEC_S, REST, SDP_TASK, _FakeRest, _StubConnect, _patched,
)

PUBLISHED = os.path.join(STUDY, "config", "results_schema.json")


def _published_schema():
    with open(PUBLISHED) as f:
        return json.load(f)


def _cfg(instances, rest):
    return runner.StudyConfig(
        base_model_id="claude-sonnet-4-6",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(instances, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://x:1/", spark_rest_url=rest)


def _brain(task, a, seed):
    return ScriptedBrain([{"code": "@dp.table\ndef t(): return spark.read.text('x')\n",
                           "command": "cmd"}])


# ---------------------------------------------------------------------------
def test_published_schema_matches_runtime_schema():
    """results_schema.json is the serialized RESULTS_JSON_SCHEMA -- they must not drift
    (the checked-in file is the published contract the analysis layer reads)."""
    assert _published_schema() == RESULTS_JSON_SCHEMA, (
        "results_schema.json has drifted from harness.schema.RESULTS_JSON_SCHEMA; "
        "regenerate it: json.dump(RESULTS_JSON_SCHEMA, f, indent=2)")


def test_published_schema_internally_consistent_with_dataclass():
    """Every ResultRow field is a published property (additionalProperties:false would
    otherwise reject emitted rows), and the measured/required stance is the one the
    None-sentinel fix declared: executor_seconds nullable + NOT required;
    executor_seconds_wallclock the required, always-present §8 anchor."""
    pub = _published_schema()
    fields = {f.name for f in dataclasses.fields(ResultRow)}
    assert fields - set(pub["properties"]) == set(), "ResultRow field missing from schema properties"
    assert pub["properties"]["executor_seconds"]["type"] == ["number", "null"]
    assert "executor_seconds" not in pub["required"]
    assert "executor_seconds_wallclock" in pub["required"]
    assert pub["properties"]["executor_seconds_wallclock"]["type"] == "number"
    for nullable in ("cpu_seconds", "cpu_seconds_to_correct", "executor_seconds_wallclock_to_correct"):
        assert pub["properties"][nullable]["type"] == ["number", "null"], nullable
    assert pub["additionalProperties"] is False


def _emit_measured_row(tmp):
    """A fully-MEASURED execute row (arm B1, mocked stage-diff REST)."""
    restore = _patched(_FakeRest())
    try:
        arm = load_arms(os.path.join(STUDY, "arms"))["B1"]

        def make_executor(task, a, seed):
            return _StubConnect(REST, 2.0)

        row = runner.run_cell(SDP_TASK, arm, 42, _cfg(4, REST), _brain, make_executor,
                              work_dir=tmp, clock=1750000000.0)
    finally:
        restore()
    return json.loads(row.to_json())


def _emit_unmeasured_gated_row(tmp):
    """An UNMEASURED GATED row (arm B): a gate-intercepted failure + an execute whose
    stage-diff falls back to (None, None) -> executor_seconds/cpu_seconds null."""
    arm = load_arms(os.path.join(STUDY, "arms"))["B"]
    assert arm.dry_run_gate

    class _GatedStub(_StubConnect):
        def __init__(self):
            super().__init__(None, 2.0)   # rest_url=None -> stage-diff (None, None)
            self._gate_calls = 0

        def run_gate(self, proposal, arm, state):
            self._gate_calls += 1
            if self._gate_calls == 1:
                return GateOutcome(failed=True, wall_s=8.0, error_class="ERR", log="rejected")
            return GateOutcome(failed=False, wall_s=8.0, log="ok")

    def make_executor(task, a, seed):
        return _GatedStub()

    row = runner.run_cell(SDP_TASK, arm, 42, _cfg(4, None), _brain, make_executor,
                          work_dir=tmp, clock=1750000000.0)
    return json.loads(row.to_json())


def test_emitted_measured_row_validates_against_published_schema():
    with tempfile.TemporaryDirectory() as tmp:
        d = _emit_measured_row(tmp)
    # measured surfaces are real numbers
    assert abs(d["executor_seconds"] - EXPECT_EXEC_S) < 1e-9
    assert abs(d["cpu_seconds"] - EXPECT_CPU_S) < 1e-9
    # validates against BOTH the published file and the code validator
    jsonschema.validate(d, _published_schema())
    assert validate_row(d) == [], validate_row(d)


def test_emitted_unmeasured_gated_row_validates_against_published_schema():
    """The row the stale schema would have REJECTED: executor_seconds null + the new
    compute fields present, on a gated arm. Must pass the PUBLISHED contract."""
    with tempfile.TemporaryDirectory() as tmp:
        d = _emit_unmeasured_gated_row(tmp)
    # the unmeasured shape: null measured surfaces, present wall-clock cross-check
    assert d["executor_seconds"] is None
    assert d["cpu_seconds"] is None
    assert d["cpu_seconds_to_correct"] is None
    assert isinstance(d["executor_seconds_wallclock"], (int, float))
    # the actual emitted row validates against the published machine-readable schema
    jsonschema.validate(d, _published_schema())
    assert validate_row(d) == [], validate_row(d)


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
