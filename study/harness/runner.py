"""Multi-arm agent runner (pre-reg §3/§5/§6/§8).

Owns the experiment's controlled machinery so no backend can confound it:

  * the loop (propose -> [gate] -> execute -> feedback), capped at the shared
    max_iterations;
  * the cost model (dry-run gate = $0 driver-only; execute = measured
    executor-seconds x price);
  * the BLIND grader call (the arm label never reaches oracles.grade_run);
  * the schema-stable results.jsonl row + the env metadata sidecar.

The ONLY thing that varies per arm is the arm manifest's loop fields, and
`arm_manifest.load_arms()` has already asserted every other field is identical
across arms. The base model, task prompt, sampling params, and per-seed dataset
all come from the shared `StudyConfig` -- physically one source -- so a
difference in outcome is attributable to the loop.

Backends are pluggable (replay for offline validation; live for the real sweep).
This file runs end-to-end offline with the replay backend; the live path needs
the Connect backend up + an API key and is exercised by the same loop code.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, cast

# allow `python harness/runner.py` and `python -m harness.runner`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from harness import cost as costmod  # noqa: E402
from harness import oracles as oraclesmod  # noqa: E402
from harness import program_metrics as pmetrics  # noqa: E402
from harness.arm_manifest import ArmManifest, load_arms, sampling_kwargs  # noqa: E402
from harness.backends.base import (AGENT_EXEC_TIMEOUT_S, EXECUTION_TIMEOUT_CLASS,  # noqa: E402
                                   HarnessFault, LoopState, ProposeError)
from harness.schema import ResultRow, validate_row  # noqa: E402
from harness import harness_faults as hf  # noqa: E402  -- retry/quarantine/breaker policy

HARNESS_VERSION = "1.0.0"
def _find_repo_root():
    # The data generators (generators/) and .git live at the repo root. The original layout
    # put harness/ three levels deep; when the study dir is relocated (e.g. into the paper
    # repo, two levels deep) a fixed ../../.. points above the repo. Walk up to the dir
    # that actually holds generators/ (or the pre-reorganization infra/) or .git so input-gen
    # paths and provenance resolve either way.
    env = os.environ.get("STUDY_REPO_ROOT")
    if env:
        return os.path.abspath(env)
    here = os.path.dirname(os.path.abspath(__file__))
    d = here
    for _ in range(6):
        d = os.path.dirname(d)
        if (os.path.isdir(os.path.join(d, "generators"))
                or os.path.isdir(os.path.join(d, "infra"))
                or os.path.isdir(os.path.join(d, ".git"))):
            return d
    return os.path.normpath(os.path.join(here, "..", ".."))
REPO_ROOT = _find_repo_root()

# HARD per-cell wall-clock cap (seconds). A single (task, arm, seed) cell that exceeds
# this is ABANDONED -- the in-process imperative JVM is force-killed, a bounded
# harness_error timeout row is recorded, and the sweep advances to the next cell. This
# is the last-resort backstop that guarantees forward progress across all 66 cells even
# if one cell wedges in a way the finer-grained in-executor bounds miss (e.g. a hang
# during profile building outside the executor). Set WELL above the per-iteration exec
# budget (AGENT_EXEC_TIMEOUT_S x max_iterations, plus LLM latency) so a legitimately slow
# cell is never killed; override with --per-cell-timeout. It is a wedge guard, not a
# normal-runtime limit.
PER_CELL_WALLCLOCK_TIMEOUT_S = 1800


# ---------------------------------------------------------------------------
# Shared (controlled) configuration -- identical for every arm
# ---------------------------------------------------------------------------
@dataclass
class StudyConfig:
    base_model_id: str
    task_prompt_path: str            # the ONE shared task prompt template
    executor_config: costmod.ExecutorConfig
    spark_remote: str = "sc://localhost:15002"
    spark_rest_url: Optional[str] = None
    image_digest: str = "uncontainerized"
    generator: str = "generators/gen_messy_orders.py"
    # Cluster-reachable storage base (e.g. s3a://.../warehouse). When set, the live
    # ConnectExecutor stages each seed's input here over the Connect channel and SDP
    # specs store here, so the REMOTE k8s executors can read input + write output.
    # None -> local file:// (in-process local executor / offline tests). pre-reg
    # logic unchanged; this only moves WHERE the data lives (DEVIATIONS D-3).
    warehouse_uri: Optional[str] = None
    # SDP spec catalog/database the generated `spark-pipeline.yml` declares. Omitting
    # these made the Connect server receive empty defaults -> PARSE_EMPTY_STATEMENT on
    # the SDP gate; the values below are proven-good on the live Connect endpoint and
    # overridable from the study/live config (`sdp_catalog`/`sdp_database` keys).
    sdp_catalog: str = "spark_catalog"
    sdp_database: str = "default"

    @staticmethod
    def from_file(path: str) -> "StudyConfig":
        with open(path) as f:
            d = json.load(f)
        return StudyConfig(
            base_model_id=d["base_model_id"],
            task_prompt_path=d["task_prompt_path"],
            executor_config=costmod.ExecutorConfig.from_dict(d["executor_config"]),
            spark_remote=d.get("spark_remote", "sc://localhost:15002"),
            spark_rest_url=d.get("spark_rest_url"),
            image_digest=d.get("image_digest", "uncontainerized"),
            generator=d.get("generator", "generators/gen_messy_orders.py"),
            warehouse_uri=d.get("warehouse_uri"),
            sdp_catalog=d.get("sdp_catalog", "spark_catalog"),
            sdp_database=d.get("sdp_database", "default"),
        )


@dataclass
class EpisodeResult:
    completed: bool
    green_iter_index: Optional[int]
    iter_costs: List[costmod.IterationCost]
    per_iteration: List[Dict[str, Any]]
    analysis_log: str
    runtime_log: str
    green_exec: Optional[Any]              # the ExecOutcome of the green iteration (for table read-back)
    exit_class: str
    # H5 conciseness: the agent-authored source of the FINAL ACCEPTED program -- the
    # proposal that reached the COMPLETED output (green iteration). None if the run
    # never completed (no accepted program to measure). For SDP arms this is the
    # `transformations/pipeline.py` @dp module; for imperative arms the `pipeline.py`
    # program. The harness-emitted SDP `spark-pipeline.yml` is NOT part of it (see
    # harness/program_metrics.py for the .yml exclusion rationale).
    final_program: Optional[str] = None
    input_tokens: int = 0                  # total LLM input tokens across the episode (Part A.5)
    output_tokens: int = 0                 # total LLM output tokens across the episode (Part A.5)


# Extra, actionable guidance fed back to the agent for failure classes where the bare
# error code is not self-explanatory. EXECUTION_TIMEOUT in particular must steer the
# agent away from the unbounded-streaming pattern that wedged the live calibration.
_FAILURE_GUIDANCE = {
    "EXECUTION_TIMEOUT": (
        "Execution exceeded the time limit and was terminated. Your job must run to "
        "COMPLETION on the provided bounded input — do NOT start an unbounded streaming "
        "query or call awaitTermination(); use a finite batch read/transform/write."),
}


def _failure_feedback(stage: str, error_class: Optional[str]) -> str:
    """`[stage] ERROR_CLASS` plus any actionable guidance for that class."""
    msg = f"[{stage}] {error_class}"
    guidance = _FAILURE_GUIDANCE.get(error_class or "")
    return f"{msg} — {guidance}" if guidance else msg


# ---------------------------------------------------------------------------
# The loop -- identical control flow for every arm; only the manifest differs
# ---------------------------------------------------------------------------
def run_episode(brain, executor, arm: ArmManifest, state: LoopState,
                cfg: StudyConfig) -> EpisodeResult:
    iter_costs: List[costmod.IterationCost] = []
    per_iteration: List[Dict[str, Any]] = []
    analysis_logs: List[str] = []
    runtime_logs: List[str] = []
    green_iter_index: Optional[int] = None
    green_exec: Optional[Any] = None
    green_code: Optional[str] = None
    completed = False
    exit_class = "max_iterations"
    propose_aborted = False
    ep_input_tokens = 0
    ep_output_tokens = 0

    for it in range(arm.max_iterations):
        # PER-ITERATION CRASH-SAFETY (deliverable 2 + 3): brain.propose() is the
        # uncontrolled, network-bound LLM step. A ProposeError (PROPOSE_TIMEOUT after
        # the harness wall-clock kill, PROPOSE_RATE_LIMIT/PROPOSE_API_ERROR after the
        # client's retries) -- or ANY other unexpected exception -- is caught here,
        # recorded as a $0 FAILED iteration, and ENDS the episode gracefully with that
        # exit_class. Catching at this altitude PRESERVES the accounting of every
        # iteration that already ran this episode (a propose blip mid-run does not
        # discard the real compute already spent). The per-cell net in main() is the
        # backstop for failures OUTSIDE this loop. Either way the batch never dies.
        try:
            proposal = brain.propose(state, arm)
        except Exception as e:  # noqa: BLE001 -- nothing from propose may abort the batch
            ec = getattr(e, "exit_class", None) or ProposeError.exit_class
            iter_costs.append(costmod.no_code_iteration_cost())  # nothing ran -> $0
            per_iteration.append({
                "iter": it, "command": "",
                "propose_error": {"exit_class": ec, "type": type(e).__name__,
                                  "message": str(e)[:500]},
                # mirror the no-code/timeout shape so failing_iterations counts it.
                "execute": {"failed": True, "completed": False, "error_class": ec},
            })
            state.add_feedback(f"[propose] {ec}: {type(e).__name__}: {str(e)[:200]}")
            exit_class = ec
            propose_aborted = True
            _advance(executor)
            break
        state.history.append(proposal)
        rec: Dict[str, Any] = {"iter": it, "command": proposal.command}
        # Part A.5: persist this turn's LLM token usage into the per-iteration record
        # (and accumulate the episode totals stamped onto the ResultRow). Always present
        # so a buggy/throttled cell's cost is auditable from the artifact; 0 for the
        # replay/scripted brains that consume no tokens.
        rec["tokens"] = {"input": int(getattr(proposal, "input_tokens", 0) or 0),
                         "output": int(getattr(proposal, "output_tokens", 0) or 0)}
        ep_input_tokens += rec["tokens"]["input"]
        ep_output_tokens += rec["tokens"]["output"]
        if getattr(proposal, "stop_reason", None) is not None:
            rec["stop_reason"] = proposal.stop_reason

        # --- NO-CODE guard: an agent turn that produced no fenced code block can't
        # be materialized or run; opening the (un-written) pipeline.py /
        # spark-pipeline.yml would raise FileNotFoundError and KILL the whole run.
        # Record a graceful FAILED iteration, feed the reason back, and let the agent
        # retry on the next turn. (Truncation -> stop_reason="max_tokens" is the
        # usual cause; it is captured above and addressed by the raised max_tokens.)
        if not (getattr(proposal, "code", "") or "").strip():
            iter_costs.append(costmod.no_code_iteration_cost())
            rec["execute"] = {"failed": True, "completed": False,
                              "error_class": "NO_CODE_PRODUCED"}
            per_iteration.append(rec)
            state.add_feedback(
                "[no-code] No fenced ```python code block found in your response — "
                "emit your module inside a single ```python ... ``` block.")
            _advance(executor)
            continue

        # B2: write the agent's generated code into the workspace BEFORE running
        # anything, so the gate and the executor act on the ACTUAL agent artifact.
        # The COMPLETE per-arm file contract is written here (SDP spec + transform,
        # or imperative pipeline + analyze-only), driven by the arm's loop config.
        _materialize_proposal(state, proposal, arm)

        # --- structural gate (only if the arm's loop has one) -----------
        if arm.dry_run_gate:
            gate = executor.run_gate(proposal, arm, state)
            analysis_logs.append(gate.log)
            # A gate that was HARD-KILLED at the timeout (e.g. an imperative
            # --analyze-only that hung) is a $0 failed iteration, NOT a structural
            # dry-run intercept -- account it like a timeout, not a gate intercept.
            if gate.error_class == EXECUTION_TIMEOUT_CLASS:
                iter_costs.append(costmod.timeout_iteration_cost(gate.wall_s))
            else:
                iter_costs.append(costmod.dry_run_iteration_cost(gate.wall_s, gate.failed))
            rec["gate"] = {"failed": gate.failed, "error_class": gate.error_class}
            if gate.failed:
                state.add_feedback(_failure_feedback("dry-run gate", gate.error_class))
                per_iteration.append(rec)
                _advance(executor)
                continue

        # --- execute on the cluster -------------------------------------
        exec_out = executor.run_execute(proposal, arm, state)
        runtime_logs.append(exec_out.log)
        if exec_out.error_class == EXECUTION_TIMEOUT_CLASS:
            # HARD-KILLED at the timeout (watchdog / process-group kill): no
            # attributable compute, so account ZERO cost -- do NOT let
            # execute_iteration_cost price the ~timeout wall_s as fake executor-seconds.
            # Still a failing iteration; NOT a dry-run intercept.
            iter_costs.append(costmod.timeout_iteration_cost(exec_out.wall_s))
        else:
            iter_costs.append(costmod.execute_iteration_cost(
                exec_out.wall_s, cfg.executor_config, exec_out.failed,
                measured_executor_seconds=exec_out.executor_seconds,
                measured_cpu_seconds=exec_out.cpu_seconds,
            ))
        rec["execute"] = {"failed": exec_out.failed, "completed": exec_out.completed,
                          "error_class": exec_out.error_class}
        per_iteration.append(rec)
        _advance(executor)

        if exec_out.failed:
            state.add_feedback(_failure_feedback("runtime", exec_out.error_class))
            continue

        if exec_out.completed:
            completed = True
            green_iter_index = len(iter_costs) - 1
            green_exec = exec_out
            # H5: capture the agent-authored source that produced the COMPLETED
            # output -- the FINAL ACCEPTED program. This is a read of the proposal
            # already in hand (no new gate/execute work); the SDP gate/dry-run code
            # path is untouched.
            green_code = getattr(proposal, "code", None)
            exit_class = "completed"
            break

    if not completed and not propose_aborted and exit_class != "completed":
        # distinguish "tried and failed every time" from "never produced output".
        # Skipped when the episode was ended by a propose failure -- exit_class already
        # carries the precise PROPOSE_*/HARNESS_EXCEPTION class and must not be relabeled.
        last_failed = any(("execute" in r and r["execute"]["failed"]) or
                          ("gate" in r and r["gate"]["failed"]) for r in per_iteration)
        exit_class = "max_iterations" if last_failed else "harness_error"

    return EpisodeResult(
        completed=completed,
        green_iter_index=green_iter_index,
        iter_costs=iter_costs,
        per_iteration=per_iteration,
        analysis_log="\n".join(analysis_logs),
        runtime_log="\n".join(runtime_logs),
        green_exec=green_exec,
        exit_class=exit_class,
        final_program=green_code,
        input_tokens=ep_input_tokens,
        output_tokens=ep_output_tokens,
    )


def _materialize_proposal(state: "LoopState", proposal, arm) -> List[str]:
    """Write the COMPLETE workspace contract each arm's backend path reads (B2).

    The set of files is driven by the arm's LOOP config (paradigm + gate), so the
    file each backend opens always exists AND holds the AGENT'S code -- never a
    stale fixed file and never a baked answer.

      Arm A  (imperative, no gate):  pipeline.py   (agent program, VERBATIM)
      Arm B2 (imperative + gate):    pipeline.py   (agent program, VERBATIM)
      Arm B  (SDP + gate):           spark-pipeline.yml  +  transformations/pipeline.py
      Arm B1 (SDP, no gate):         spark-pipeline.yml  +  transformations/pipeline.py

    IMPERATIVE arms (A, B2): the agent OWNS the program. `pipeline.py` is exactly
    `proposal.code` -- the harness injects NO SparkSession, no materialize-main, and
    no analyze-only harness. The agent acquires its own session, reads
    AGENT_INPUT_PATH, writes the final GOLD output as parquet to AGENT_OUTPUT_PATH
    (no saveAsTable/catalog), prints "Run is COMPLETED", and supports
    `--analyze-only` for the gate. This removes a validity confound: an imperative
    failure is attributable to the AGENT'S authored code, not to harness scaffolding.

    SDP arms (B, B1) are UNCHANGED in the agent-owned sense: only `proposal.code` is
    content; the SDP spec is pure boilerplate (no agent logic, no leaked answer), and
    it now declares catalog/database threaded from the study config (#15). Both the
    SDP gate AND SDP execute read `spark-pipeline.yml` via the Python CLI with --spec
    (`cli.py dry-run --spec` / `cli.py run --spec`), which globs `transformations/**`.
    Imperative execute runs the agent's `pipeline.py`; the B2 gate runs
    `pipeline.py --analyze-only`. See harness/backends/live.py.
    """
    if not getattr(proposal, "code", None):
        return []
    written = materialize_workspace(state.workspace, proposal.code, arm.paradigm,
                                    bool(arm.dry_run_gate), state.task, arm.arm_id,
                                    state.dataset_path, state.output_table,
                                    storage_uri=state.output_storage,
                                    catalog=state.sdp_catalog, database=state.sdp_database)
    _assert_materialized(state, arm, written)
    return written


# Files each paradigm's backend opens; their ABSENCE after a non-empty materialization
# is an INSTRUMENT failure (a failed/relative write), not an agent outcome (Part A.3).
_REQUIRED_FILES = {
    "sdp": ("spark-pipeline.yml", os.path.join("transformations", "pipeline.py")),
    "imperative": ("pipeline.py",),
}


def _assert_materialized(state: "LoopState", arm, written: List[str]) -> None:
    """Post-materialization existence check (Part A.3): after writing a NON-EMPTY
    proposal, assert the files the backend will open ACTUALLY exist on disk, resolved
    ABSPATH-based. A missing required file here means the harness failed to materialize
    the workspace it promised (a bad path, a partial write) -- so raise a HarnessFault
    (an instrument failure that quarantines the cell) rather than letting the executor
    hand a non-existent path to the CLI and have the resulting
    PIPELINE_SPEC_FILE_DOES_NOT_EXIST scored as an AGENT failure."""
    required = _REQUIRED_FILES.get("sdp" if arm.paradigm == "sdp" else "imperative", ())
    ws = os.path.abspath(state.workspace)
    missing = [rel for rel in required if not os.path.isfile(os.path.join(ws, rel))]
    if missing:
        raise HarnessFault(
            f"materialization did not produce required {arm.paradigm} file(s) "
            f"{missing} under {ws!r} (wrote {written}); the harness failed to stage "
            f"the workspace -- not an agent outcome.",
            reason="SDP_MATERIALIZATION_MISSING" if arm.paradigm == "sdp"
            else "MATERIALIZATION_MISSING")


def materialize_workspace(ws: str, code: str, paradigm: str, dry_run_gate: bool,
                          task: str, arm_id: str, dataset_path: str,
                          output_table: str, storage_uri: Optional[str] = None,
                          catalog: str = "spark_catalog",
                          database: str = "default") -> List[str]:
    """Write the per-arm workspace files; return the relative paths written.

    `dataset_path` is the path the agent + gate read -- on the live cluster this is
    already the STAGED remote path (the runner rewrites it before materializing), so
    `spark.read.text(...)` inside the agent code resolves on the k8s executors.
    `storage_uri` is the SDP spec `storage:` base; cluster-reachable on the live
    cluster, `file://{ws}/pipeline-storage` locally. Split out so the per-arm
    contract can be unit-tested deterministically (tests/test_workspace_contract).
    """
    written: List[str] = []

    def _w(rel: str, content: str):
        path = os.path.join(ws, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(rel) else None
        with open(path, "w") as f:
            f.write(content)
        written.append(rel)

    if paradigm == "sdp":
        # SDP project: the agent's @dp.table module under the glob + the spec.
        _w(os.path.join("transformations", "pipeline.py"), code)
        _w("spark-pipeline.yml", _sdp_spec(task, arm_id, ws, storage_uri,
                                           catalog=catalog, database=database))
    else:
        # imperative: the agent OWNS the program. Write `proposal.code` VERBATIM --
        # no harness SparkSession, no materialize-main, no analyze-only harness. The
        # agent acquires its own session, reads AGENT_INPUT_PATH, writes the final
        # GOLD output as parquet to AGENT_OUTPUT_PATH (catalog-free), prints
        # "Run is COMPLETED", and supports `--analyze-only` for the gate. Paths are
        # delivered at run time via env, NOT baked into the file.
        _w("pipeline.py", code)
    return written


def _sdp_spec(task: str, arm_id: str, ws: str, storage_uri: Optional[str] = None,
             catalog: str = "spark_catalog", database: str = "default") -> str:
    """A standard SDP project manifest -- pure boilerplate, no agent logic.
    `storage` is the cluster-reachable warehouse on the live cluster, else local.

    `catalog`/`database` MUST be emitted: omitting them made the Connect server
    receive empty catalog/database defaults and fail the SDP gate with
    PARSE_EMPTY_STATEMENT. `spark_catalog`/`default` are proven-good on the live
    endpoint (overridable via the study config's sdp_catalog/sdp_database).

    NO `configuration:` block: pinning e.g. spark.sql.session.timeZone here would
    apply ONLY to the SDP arms (B/B1) and not the imperative arms (A/B2), an
    asymmetric advantage that also hands the SDP arms correct UTC behavior for free
    -- masking the UTC-normalization silent defect the oracle is designed to catch
    (a scientific confound biasing H1). UTC handling is the agent's job, per arm."""
    storage = storage_uri or f"file://{ws}/pipeline-storage"
    return (
        f"name: {task}__{arm_id}\n"
        f"storage: {storage}\n"
        f"catalog: {catalog}\n"
        f"database: {database}\n"
        "libraries:\n"
        "  - glob:\n"
        "      include: transformations/**\n"
    )


def _advance(executor) -> None:
    adv = getattr(executor, "advance", None)
    if callable(adv):
        adv()


def _profile_from_metrics(metrics: Optional[Dict[str, Any]]) -> Optional[oraclesmod.OutputProfile]:
    """Replay/offline shortcut: build a profile from canned residual metrics.
    Real executors never use this -- they read the materialized table (B1)."""
    if metrics is None:
        return None
    p = oraclesmod.OutputProfile()
    for k, v in metrics.items():
        if hasattr(p, k):
            setattr(p, k, v)
        else:
            p.extra[k] = v
    return p


def _build_profile(ep: "EpisodeResult", executor, task_spec: Dict[str, Any],
                   dataset: Optional[str]) -> Optional[oraclesmod.OutputProfile]:
    """Construct the OutputProfile for a completed run (B1/B3).

    Three paths, all feeding the SAME grade_run:
      * SDP local (LocalConnectExecutor): builds the profile in a SUBPROCESS Connect
        helper via `build_output_profile_subprocess` -- checked FIRST so the parent
        runner process never evaluates `executor.spark` (which is GUARDED to raise
        under the local backend; classic-vs-Connect mode is process-global, Option C).
      * imperative LOCAL (LocalSparkExecutor): read the materialized parquet path
        back through the executor's IN-PROCESS `spark` session and run the OUTPUT
        oracle without touching a catalog.
      * remote-live ConnectExecutor: read the materialized table back through the
        Connect catalog (unchanged).
      * offline replay: no real executor -> canned `output_metrics` shortcut.
    """
    if not ep.completed or ep.green_exec is None:
        return None
    green = ep.green_exec
    contract = task_spec.get("output_contract")

    # SDP-local (LocalConnectExecutor): classic-vs-Connect mode is process-global, so
    # the parent runner MUST NOT evaluate `executor.spark` -- it is GUARDED to raise a
    # RuntimeError, and `getattr(executor, "spark", None)` does NOT suppress that (the
    # default only catches AttributeError), which crashed the runner on a contract-less
    # cell like p5_mart. Dispatch on the subprocess-helper CAPABILITY (unique to
    # LocalConnectExecutor) EXCLUSIVELY: build the profile in a subprocess Connect
    # session when there is a contract to grade, and otherwise return None WITHOUT ever
    # touching executor.spark. We must never fall through to the in-process spark path
    # for this executor.
    sub = getattr(executor, "build_output_profile_subprocess", None)
    if callable(sub):
        if contract and dataset:
            # REAL profile via the sanctioned subprocess Connect session (Option C).
            return sub(task_spec, dataset, contract["table"])
        # No machine-readable output_contract (e.g. p5_mart): nothing for the output
        # oracle to grade -> no profile. Structural defects are still graded from the
        # logs in grade_run, and H2 executor-seconds come from green_exec, so this loses
        # no real data.
        return None

    path_reader = getattr(executor, "read_output_path", None)
    spark = getattr(executor, "spark", None)
    if callable(path_reader) and spark is not None and contract and dataset:
        from harness import output_oracles
        # Local imperative final GOLD output is path/parquet. A D6 oracle-graded
        # SECONDARY dedup table (e.g. dedup_table=silver_orders/clean_orders, which
        # differs from table=gold_daily) is ALSO materialized to its OWN parquet path
        # -- a sibling of the primary output under the same workspace -- so D6 reads it
        # from DISK, exactly like the primary gold read-back fix. This decouples the
        # secondary grade from the agent's session catalog, which loses an in-session
        # view the moment the agent calls `spark.stop()`. `read_table` is still passed
        # for the same-table case (already disk) and as a harmless fallback; the
        # disk-path read is preferred whenever a separate dedup table exists.
        primary_path = getattr(green, "output_tables", [None])[0]
        dedup_path = local_imperative_dedup_path(contract, primary_path or "")
        secondary_reader = getattr(executor, "read_table", None)
        return output_oracles.build_output_profile(
            secondary_reader if callable(secondary_reader) else None,
            spark, dataset, task_spec["defects_in_scope"], contract,
            read_path=path_reader, output_path=primary_path, dedup_path=dedup_path)

    reader = getattr(executor, "read_table", None)
    if callable(reader) and spark is not None and contract and dataset:
        from harness import output_oracles
        return output_oracles.build_output_profile(
            reader, spark, dataset, task_spec["defects_in_scope"], contract)
    # offline / replay: canned residuals
    return _profile_from_metrics(green.output_metrics)




def _fail_incomplete_required_output(ep: "EpisodeResult",
                                     output_profile: Optional[oraclesmod.OutputProfile]) -> None:
    """A completed final output is not enough if another required graded table is
    absent. Treat that as a visible incomplete output, matching final-output
    completion checks, instead of letting default residual=0 score as clean.
    """
    if not ep.completed or output_profile is None:
        return
    err = (output_profile.extra or {}).get("required_output_read_error")
    if not err:
        return
    ep.completed = False
    ep.exit_class = "runtime_error"
    if ep.per_iteration:
        rec = ep.per_iteration[-1].setdefault("execute", {})
        rec["failed"] = True
        rec["completed"] = False
        rec["error_class"] = "REQUIRED_OUTPUT_TABLE_NOT_FOUND"
        rec["required_output_read_error"] = err
    if ep.green_iter_index is not None and ep.green_iter_index < len(ep.iter_costs):
        ep.iter_costs[ep.green_iter_index].failed = True
    ep.green_iter_index = None
    # the green output was retracted -> there is no FINAL ACCEPTED program to measure.
    ep.final_program = None
    if ep.green_exec is not None:
        ep.green_exec.failed = True
        ep.green_exec.completed = False
        ep.green_exec.error_class = "REQUIRED_OUTPUT_TABLE_NOT_FOUND"
        ep.green_exec.log = (f"{ep.green_exec.log}\n"
                             f"[completion-check] required graded table not readable: {err}")

# ---------------------------------------------------------------------------
# Remote data staging (D-3): make the per-seed input + SDP storage cluster-readable
# ---------------------------------------------------------------------------
def _stage_input(executor, local_path: Optional[str], subkey: Optional[str] = None) -> Optional[str]:
    """Stage the local input where THIS run's executor can read it.

    A real ConnectExecutor implements `stage_input(local, subkey=None) -> remote`
    (copies the NDJSON over the Connect channel to cluster-reachable storage and
    returns the remote path). The in-process LocalSparkExecutor and the offline
    ReplayExecutor expose no `stage_input`, so the local path is used unchanged --
    local behaviour is untouched.

    `subkey` (used for AUX inputs) lands each additional input under its own
    sub-path of the cell's staging base, so multiple staged inputs never collide.
    The primary input passes subkey=None and keeps its original staging path, so
    single-input arms are byte-for-byte unchanged.
    """
    if not local_path:
        return local_path
    stage: Any = getattr(executor, "stage_input", None)
    if not callable(stage):
        return local_path
    staged = stage(local_path) if subkey is None else stage(local_path, subkey)
    return cast(Optional[str], staged)


def _stage_aux_inputs(executor, aux_local: Dict[str, str]) -> Dict[str, str]:
    """Stage every AUX input the same way the primary is staged, each under its own
    subkey so they never collide on the remote staging base. Identical across arms."""
    out: Dict[str, str] = {}
    for name, local in (aux_local or {}).items():
        staged = _stage_input(executor, local, subkey=name)
        out[name] = staged or local
    return out


def _sdp_storage_for(cfg: StudyConfig, task: str, arm_id: str, seed: int, ws: str) -> str:
    """SDP spec `storage:` base. Cluster-reachable on the live cluster (so the k8s
    executors can write the pipeline's tables), local file:// otherwise."""
    if cfg.warehouse_uri:
        return f"{cfg.warehouse_uri.rstrip('/')}/_ssa_pipeline_storage/{task}/{arm_id}/seed{seed}"
    return f"file://{ws}/pipeline-storage"


# ---------------------------------------------------------------------------
# Dataset generation (B4: per-TASK generator, per seed, identical across arms)
# ---------------------------------------------------------------------------
def aux_input_name(generator_path: str) -> str:
    """The neutral logical name an aux input is exposed under (the generator stem
    with the `gen_` prefix stripped). `infra/gen_fx_rates_cdc.py` -> `fx_rates_cdc`.
    Used as the AGENT_AUX_INPUTS key and the name in the prompt; carries a location,
    never a fix/technique (covered by the prompt-no-leak guard)."""
    stem = os.path.splitext(os.path.basename(generator_path))[0]
    return stem[len("gen_"):] if stem.startswith("gen_") else stem


def _generate_one(inp: str, extra_args: List[str], seed: int,
                  out_dir: str, owner: str) -> Optional[str]:
    """Generate ONE registered input (`inp`, a `.py` generator) at this seed into
    `out_dir`, returning the local NDJSON path. Non-`.py` inputs (upstream tables)
    produce no standalone dataset and return None. Deterministic per seed, so every
    arm gets byte-identical data."""
    if not inp.endswith(".py"):
        return None  # cross-pipeline / upstream-table input; no generator
    if inp.startswith("infra/"):
        # TASKS.lock.json and the seed subsets are frozen byte-for-byte with the
        # pre-reorganization layout (generators lived under infra/). Map the legacy
        # prefix instead of editing the frozen corpus.
        inp = "generators/" + inp[len("infra/"):]
    gen = os.path.join(REPO_ROOT, inp)
    if not os.path.exists(gen):
        raise FileNotFoundError(f"task {owner!r} input generator not found: {gen}")
    stem = os.path.splitext(os.path.basename(inp))[0]
    out = os.path.join(out_dir, f"{stem}_seed{seed}.ndjson")
    if not os.path.exists(out) or os.path.getsize(out) == 0:
        with open(out, "w") as fo, open(out + ".profile", "w") as fe:
            subprocess.run([sys.executable, gen, "--seed", str(seed)] + list(extra_args),
                           stdout=fo, stderr=fe, check=True)
    return out


def generate_dataset(task_spec: Dict[str, Any], cfg: StudyConfig, seed: int,
                     out_dir: str) -> Optional[str]:
    """Generate THIS task's registered PRIMARY input at this seed (B4).

    Each task in TASKS.lock declares its own `input` generator (orders / CDC /
    payments). A matched seed gives every arm byte-identical data. Tasks whose
    input is an upstream pipeline's published table (e.g. p5_mart reads
    p2_cdc.customers_current) declare a non-`.py` input and produce no standalone
    dataset here -- they read the upstream table at run time; we return None and
    record the dependency.
    """
    os.makedirs(out_dir, exist_ok=True)
    inp = task_spec.get("input", cfg.generator)
    # optional per-task generator flags (e.g. ["--v3"] for the orders realism
    # append); absent -> no extra args, byte-identical to the v2 invocation.
    extra = list(task_spec.get("input_args", []))
    return _generate_one(inp, extra, seed, out_dir, task_spec.get("id", inp))


def generate_aux_datasets(task_spec: Dict[str, Any], cfg: StudyConfig, seed: int,
                          out_dir: str) -> Dict[str, str]:
    """Generate THIS task's ADDITIONAL declared inputs (`aux_inputs`) at this seed.

    Corpus v3 added tasks that need more than one input (stream-stream temporal
    join, as-of SCD2 join, HC-1 trades+fx, HC-2 clicks+users-CDC). Each entry of
    `aux_inputs` is its own generator; we generate every `.py` one deterministically
    at this seed (identical across arms, exactly like the primary) and return a
    {neutral-name: local-path} map. Tasks with no aux declared get `{}` and are
    therefore byte-for-byte identical to before. Aux generators take NO per-task
    flags (those belong to the primary), so the aux data is a pure function of seed.
    """
    os.makedirs(out_dir, exist_ok=True)
    out: Dict[str, str] = {}
    for aux in (task_spec.get("aux_inputs") or []):
        local = _generate_one(aux, [], seed, out_dir, task_spec.get("id", aux))
        if local is not None:
            out[aux_input_name(aux)] = local
    return out


# ---------------------------------------------------------------------------
# Provenance helpers
# ---------------------------------------------------------------------------
def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT,
                                       text=True).strip()
    except Exception:
        return "unknown"


def spark_version() -> str:
    try:
        import pyspark
        return pyspark.__version__
    except Exception:
        return "unknown"


def now_utc_iso(clock) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(clock))


def local_imperative_output_path(arm, executor, ws: str, out_table: str) -> str:
    """The Part-1 LOCAL imperative-only path/parquet output-contract escape hatch:
    `<ws>/<out_table>.parquet` for a LOCAL imperative cell, else "" (which keeps the
    original table contract for SDP B/B1 and remote Connect imperative -- see the
    inline note in run_cell).

    Imperative arms carry paradigm == "imperative_pyspark" (A, A2, B2); SDP arms carry
    "sdp" (B, B1) -- the ONLY two VALID_PARADIGMS. The old inline gate compared against
    the literal "imperative", a value `paradigm` NEVER takes, so the branch was DEAD:
    AGENT_OUTPUT_PATH was never the contract path the oracle reconciles against, and a
    local imperative cell could fail its completion check with a spurious
    OUTPUT_PATH_NOT_FOUND. Match the EXACT manifest value `"imperative_pyspark"`: this
    captures intent precisely and -- unlike a `!= "sdp"` test -- will not silently
    catch a future third paradigm that is neither SDP nor classic imperative."""
    is_local_imperative = (arm.paradigm == "imperative_pyspark" and
                           getattr(executor, "name", "") == "local_spark")
    return os.path.join(ws, f"{out_table}.parquet") if is_local_imperative else ""


def local_imperative_dedup_path(contract: Optional[Dict[str, Any]], primary_output_path: str) -> str:
    """The LOCAL IMPERATIVE on-disk parquet path for a D6 SECONDARY dedup table, or ""
    when the task has none. Returns non-empty ONLY when the contract declares a
    `dedup_table` that DIFFERS from the primary `table` -- the exactly-two affected
    tasks (orders_silver_gold/silver_orders, p12_quarantine_dlq/clean_orders). When
    `dedup_table` equals `table` (p2/p6/p10/p13 and friends) the D6 grade already
    reads the primary output from disk, so no separate path is produced and their
    behaviour is byte-for-byte unchanged.

    The dedup parquet is a SIBLING of the primary output under the same workspace, so
    the agent's injected AGENT_DEDUP_PATH and the oracle's read-back resolve to the
    identical location by construction (BOTH call this helper). The filename is
    NEUTRAL (`secondary_output.parquet`) -- it deliberately does NOT embed the dedup
    table's contract name, keeping the imperative instruction defect-neutral (the
    existing design hides which secondary table is graded; see the no-leak guard in
    tests/test_workspace_contract). Pass "" / a non-local primary path (SDP, remote
    Connect) -> "" so only LOCAL imperative is routed to disk; those arms keep their
    live-catalog D6 read."""
    if not contract or not primary_output_path:
        return ""
    dedup_table = contract.get("dedup_table")
    if not dedup_table or dedup_table == contract.get("table"):
        return ""
    return os.path.join(os.path.dirname(primary_output_path), "secondary_output.parquet")


# ---------------------------------------------------------------------------
# One (task, arm, seed) cell -> one ResultRow
# ---------------------------------------------------------------------------
def run_cell(task_spec: Dict[str, Any], arm: ArmManifest, seed: int, cfg: StudyConfig,
             make_brain, make_executor, work_dir: str, clock: float) -> ResultRow:
    task = task_spec["id"]
    oracle_spec = oraclesmod.TaskOracleSpec(task=task, defects_in_scope=task_spec["defects_in_scope"])
    ws = os.path.join(work_dir, f"{task}__{arm.arm_id}__seed{seed}")
    os.makedirs(ws, exist_ok=True)
    dataset = generate_dataset(task_spec, cfg, seed, os.path.join(work_dir, "_data"))
    aux_datasets = generate_aux_datasets(task_spec, cfg, seed, os.path.join(work_dir, "_data"))
    contract = task_spec.get("output_contract") or {}
    out_table = contract.get("table") or f"agent_out_{task}"

    brain = make_brain(task, arm, seed)
    executor = make_executor(task, arm, seed)

    # ---- D-3: stage the per-seed input where the run's executor can read it ----
    # Local/in-process executor: data is co-located -> the local path is used as-is.
    # Remote ConnectExecutor: the k8s executors cannot see this machine's FS, so it
    # ships the rows over the Connect protocol (createDataFrame) and writes them to
    # an s3a:// path (df.write.text, executors write via IRSA), returning that
    # cluster-reachable path. The SAME staged path is threaded to BOTH the agent (the
    # input it reads, baked into pipeline.py) AND the oracle (input_path it reads for
    # ground truth), so they reference identical input. No grading logic changes.
    staged = _stage_input(executor, dataset)
    # Stage every ADDITIONAL declared input the SAME way, each under its own subkey
    # (no collision), so a multi-input task exposes ALL inputs to the agent on BOTH
    # paradigms. Identical across arms; the oracle still reads only the primary.
    staged_aux = _stage_aux_inputs(executor, aux_datasets)
    sdp_storage = _sdp_storage_for(cfg, task, arm.arm_id, seed, ws)
    # The path/parquet output contract is a Part-1 LOCAL imperative-only escape
    # hatch. Do NOT set it for SDP (B/B1) or remote Connect imperative: those arms
    # must keep their original prompt/env/table contract byte-for-byte so the
    # paradigm/backend comparison is not confounded by AGENT_OUTPUT_PATH guidance.
    # See local_imperative_output_path for the paradigm-string discrimination (the
    # old inline gate compared against a literal "imperative" that never matched).
    output_path = local_imperative_output_path(arm, executor, ws, out_table)
    # LOCAL IMPERATIVE only: pin a SEPARATE D6 dedup table to its own on-disk parquet
    # (sibling of output_path), so the agent materializes it to disk and the oracle
    # reads it from disk after the agent's `spark.stop()`. "" for same-table D6 and
    # for SDP/Connect (output_path is "" there) -> their contract is unchanged.
    dedup_path = local_imperative_dedup_path(contract, output_path)
    state = LoopState(task=task, seed=seed, workspace=ws, dataset_path=staged or "",
                      aux_inputs=staged_aux,
                      output_table=out_table, output_path=output_path,
                      dedup_path=dedup_path,
                      output_storage=sdp_storage,
                      sdp_catalog=cfg.sdp_catalog, sdp_database=cfg.sdp_database)

    try:
        ep = run_episode(brain, executor, arm, state, cfg)

        # ---- B1/B3: build the OutputProfile from the REAL materialized table -----
        # On a completed run, read the agent's output back through the executor and run
        # the task's OUTPUT oracle on the SAME staged input (state.dataset_path). This is
        # the authoritative live path, identical for local and Connect executors.
        # Replay/offline executors expose no read_table and fall back to canned metrics.
        output_profile = _build_profile(ep, executor, task_spec, state.dataset_path)
        _fail_incomplete_required_output(ep, output_profile)
    finally:
        # LocalSparkExecutor owns an in-process classic SparkSession. Stop it after
        # the profile is built so a later cell cannot inherit static catalog settings
        # from a previous session (the Hive/ObjectStore root-cause trap).
        stopper = getattr(executor, "stop", None)
        if callable(stopper):
            stopper()

    # ---- BLIND grade: arm is NOT passed to the grader -----------------
    outcome = oraclesmod.RunOutcome(
        completed=ep.completed,
        analysis_log=ep.analysis_log,
        runtime_log=ep.runtime_log,
        output=output_profile,
    )
    grade = oraclesmod.grade_run(oracle_spec, outcome)

    rc = costmod.aggregate(ep.iter_costs, ep.green_iter_index, completed=ep.completed)

    # ---- H5 conciseness: measure the FINAL ACCEPTED agent program -------------
    # `ep.final_program` is the agent-authored source that reached COMPLETED (None if
    # the run never completed). The metric is paradigm-agnostic; the SDP spec
    # spark-pipeline.yml is harness boilerplate and is intentionally NOT measured.
    cm = pmetrics.program_metrics(ep.final_program)

    row = ResultRow(
        run_id=f"{task}__{arm.arm_id}__seed{seed}",
        task=task, arm=arm.arm_id, seed=seed,
        spark_version=spark_version(),
        image_digest=cfg.image_digest,
        git_sha=git_sha(),
        base_model_id=arm.base_model_id,
        executor_config=cfg.executor_config.to_dict(),
        silent_defect=grade.silent_defect,
        defect_classes=grade.defect_classes,
        detection_stage=grade.detection_stage,
        iterations=len(ep.per_iteration),
        wall_s=rc.total_wall_s,
        executor_seconds=rc.total_executor_seconds,
        usd=rc.total_usd,
        exit_class=ep.exit_class,
        task_success=ep.completed,
        reached_correct=rc.reached_correct,
        iterations_to_green=(None if ep.green_iter_index is None
                             else _iters_to_green(ep)),
        wall_s_to_green=rc.wall_s_to_green,
        executor_seconds_to_correct=rc.executor_seconds_to_correct,
        cpu_seconds=rc.total_cpu_seconds,
        cpu_seconds_to_correct=rc.cpu_seconds_to_correct,
        executor_seconds_wallclock=rc.total_executor_seconds_wallclock,
        executor_seconds_wallclock_to_correct=rc.executor_seconds_wallclock_to_correct,
        dry_run_intercepts=rc.dry_run_intercepts,
        failing_iterations=rc.failing_iterations,
        per_defect_detection=grade.per_defect_detection,
        per_iteration=ep.per_iteration,
        input_tokens=ep.input_tokens,
        output_tokens=ep.output_tokens,
        backend=getattr(brain, "name", "unknown"),
        transcript_path=_write_transcript(ws, state, ep),
        final_program=ep.final_program,
        final_program_loc=cm["final_program_loc"],
        final_program_loc_body=cm["final_program_loc_body"],
        ast_node_count=cm["ast_node_count"],
        ast_node_count_body=cm["ast_node_count_body"],
        timestamp_utc=now_utc_iso(clock),
        notes=None,
    )
    problems = validate_row(dataclasses.asdict(row))
    if problems:
        row.notes = "SCHEMA WARNINGS: " + "; ".join(problems)
    return row


def run_cell_guarded(task_spec: Dict[str, Any], arm: ArmManifest, seed: int,
                     cfg: StudyConfig, make_brain, make_executor, work_dir: str,
                     clock: float, per_cell_timeout_s: float,
                     backend_name: str = "unknown") -> ResultRow:
    """Run one cell under a HARD per-cell wall-clock deadline -- the backstop that
    guarantees the sweep makes forward progress across every cell.

    The cell runs (via `_run_cell_safe`, so any in-episode/grading/staging exception is
    already CAUGHT and classified into a soft-failed row with the precise crash-safety
    `exit_class` + `harness_fault_reason`) in a daemon worker thread. Two failure modes
    both degrade to ONE bounded `harness_error` row so the sweep always advances -- never
    an infinite stall and never a sweep-killing crash:

      * WEDGE (the live calibration symptom: output materialized, then an unbounded
        in-process py4j call -- read-back, profile build, or SparkSession.stop -- never
        returns): the deadline fires, the cell is ABANDONED, the in-process imperative
        JVM is force-killed (which unblocks the dying worker and hands the next cell a
        clean JVM), and a PER_CELL_TIMEOUT row is recorded.
      * CRASH (a residual uncaught exception, e.g. a profile/read-back error): the
        worker captured it; rather than re-raise and kill the whole runner, we record a
        bounded CELL_ERROR row and advance. This is the LAST-RESORT safety net -- the
        primary paths (the LLM/post-exec bounds and the type-dispatched profile build)
        are expected to succeed.

    A cell that finishes cleanly returns its REAL row unchanged.
    """
    box: Dict[str, Any] = {"row": None, "err": None}

    def _t() -> None:
        try:
            # _run_cell_safe is the inner net: it returns a classified soft-failed row
            # (precise exit_class + harness_fault_reason) for any cell-level error and only
            # re-raises a deliberate operator abort. The deadline/JVM-kill backstop below
            # then handles ONLY the genuine wedge (thread still alive) or a residual escape.
            box["row"] = _run_cell_safe(task_spec, arm, seed, cfg, make_brain,
                                        make_executor, work_dir, clock,
                                        backend=backend_name)
        except BaseException as e:  # noqa: BLE001 -- surfaced to the caller below
            box["err"] = e

    th = threading.Thread(
        target=_t, daemon=True,
        name=f"cell-{task_spec.get('id')}-{getattr(arm, 'arm_id', '?')}-{seed}")
    th.start()
    th.join(per_cell_timeout_s)
    if th.is_alive():
        # Deadline breached: abandon the cell. Force-kill the in-process imperative JVM
        # (best-effort; a no-op for SDP/replay cells that have none) so the wedged
        # worker unblocks and the next local cell launches a clean JVM.
        _abandon_local_jvm_best_effort()
        return _harness_error_row(
            task_spec, arm, seed, cfg, clock, backend_name,
            wall_s=float(per_cell_timeout_s),
            note=(f"PER_CELL_TIMEOUT: cell exceeded the {per_cell_timeout_s:.0f}s hard "
                  f"per-cell wall-clock cap and was abandoned; in-process Spark JVM "
                  f"force-killed and the sweep advanced (per-iteration exec budget "
                  f"AGENT_EXEC_TIMEOUT_S={AGENT_EXEC_TIMEOUT_S}s)."))
    if box["err"] is not None:
        err = box["err"]
        # Operator interrupt / clean shutdown must propagate -- never mask it as a row.
        if isinstance(err, (KeyboardInterrupt, SystemExit)):
            raise err
        # SAFETY NET: a cell that crashed must not kill the sweep. run_cell's own
        # `finally` already tore the executor down; record a bounded row and advance.
        # An AgentApiError (infra/model failure) lands here too, so calibration can tell
        # it apart from a real arm/task outcome (max_iterations / runtime_error).
        return _harness_error_row(
            task_spec, arm, seed, cfg, clock, backend_name, wall_s=0.0,
            note=(f"CELL_ERROR: cell raised {type(err).__name__}: "
                  f"{str(err)[:500]} -- recorded as a bounded error row and the sweep "
                  f"advanced (safety net; the primary path should not raise)."),
            # Preserve the crash type+message in a structured, machine-greppable field
            # (NOT only truncated into `notes`): e.g. an "Only one SparkContext should
            # be running in this JVM" SparkException surfaces here verbatim.
            error=f"{type(err).__name__}: {err}")
    return cast(ResultRow, box["row"])


def _abandon_local_jvm_best_effort() -> None:
    """Force-kill + neutralize the active in-process imperative executor, if any."""
    try:
        from harness.backends.local import abandon_active_local_executor
        abandon_active_local_executor()
    except Exception:  # noqa: BLE001
        pass


def _harness_error_row(task_spec: Dict[str, Any], arm: ArmManifest, seed: int,
                       cfg: StudyConfig, clock: float, backend_name: str,
                       wall_s: float, note: str, error: str = "") -> ResultRow:
    """The bounded row recorded when the per-cell guard abandons a cell (wedge) or
    catches a residual crash. A harness-level event (not an agent outcome):
    exit_class=harness_error, no silent defect, zero attributable cost."""
    task = task_spec["id"]
    row = ResultRow(
        run_id=f"{task}__{arm.arm_id}__seed{seed}",
        task=task, arm=arm.arm_id, seed=seed,
        spark_version=spark_version(),
        image_digest=cfg.image_digest,
        git_sha=git_sha(),
        base_model_id=arm.base_model_id,
        executor_config=cfg.executor_config.to_dict(),
        silent_defect=False, defect_classes=[], detection_stage="n/a",
        iterations=0, wall_s=wall_s,
        executor_seconds=None, usd=0.0,
        exit_class="harness_error",
        task_success=False, reached_correct=False,
        executor_seconds_wallclock=0.0,
        backend=backend_name,
        transcript_path=None,
        timestamp_utc=now_utc_iso(clock),
        notes=note,
        error=error,
    )
    problems = validate_row(dataclasses.asdict(row))
    if problems:  # should never happen; surface it rather than emit a silent bad row
        row.notes = (row.notes or "") + " | SCHEMA WARNINGS: " + "; ".join(problems)
    return row


def _failed_cell_row(task_spec: Dict[str, Any], arm: ArmManifest, seed: int,
                     cfg: StudyConfig, clock: float, exit_class: str, message: str,
                     backend: str = "unknown",
                     harness_fault_reason: Optional[str] = None,
                     wall_s: float = 0.0, note_prefix: str = "CELL FAILED SOFT",
                     error: str = "") -> ResultRow:
    """A schema-valid, SOFT-FAILED ResultRow for a cell that raised OUTSIDE the
    episode loop (factory construction, staging, grading, schema, or any unexpected
    error) -- the per-cell safety net's record so the sweep can continue. Compute
    fields are zero/None (nothing attributable ran); `exit_class` carries the precise
    crash-safety class and the failure is preserved in `notes`. `harness_fault_reason`
    carries the SPECIFIC instrument-fault token (e.g. a HarnessFault's SDP_SPEC_MISSING)
    so the quarantine report keeps it even though exit_class is the unified bucket.
    `error` is main's structured, machine-greppable crash detail (exception type+message),
    kept SEPARATE from the truncating `notes` -- empty for non-crash callers."""
    task = task_spec["id"]
    row = ResultRow(
        run_id=f"{task}__{arm.arm_id}__seed{seed}",
        task=task, arm=arm.arm_id, seed=seed,
        spark_version=spark_version(),
        image_digest=cfg.image_digest,
        git_sha=git_sha(),
        base_model_id=arm.base_model_id,
        executor_config=cfg.executor_config.to_dict(),
        silent_defect=False,
        defect_classes=[],
        detection_stage="n/a",
        iterations=0,
        wall_s=wall_s,
        executor_seconds=None,
        usd=0.0,
        exit_class=exit_class,
        task_success=False,
        reached_correct=False,
        per_iteration=[],
        harness_fault_reason=harness_fault_reason,
        backend=backend,
        transcript_path=None,
        timestamp_utc=now_utc_iso(clock),
        notes=f"{note_prefix} ({exit_class}): {message}"[:1000],
        error=error,
    )
    return row


def _run_cell_safe(task_spec: Dict[str, Any], arm: ArmManifest, seed: int,
                   cfg: StudyConfig, make_brain, make_executor, work_dir: str,
                   clock: float, backend: str = "unknown") -> ResultRow:
    """run_cell under the ULTIMATE per-cell safety net (deliverable 2): ANY exception
    -- a propose API error/timeout that escaped the episode-level handler, an executor
    or staging failure, a grading/schema error, or anything unexpected -- is CAUGHT,
    converted into a structured failed ResultRow, and RETURNED (never raised). One bad
    cell is recorded and the multi-hour/multi-day sweep continues to the next cell.

    The common propose failures are already classified WITHIN run_episode (so their
    rows keep full per-iteration accounting); this net preserves the SAME exit_class
    vocabulary via `e.exit_class` for consistency, defaulting unexpected escapes to
    HARNESS_EXCEPTION."""
    try:
        return run_cell(task_spec, arm, seed, cfg, make_brain, make_executor,
                        work_dir, clock)
    except (KeyboardInterrupt, SystemExit):
        # a DELIBERATE operator abort of the whole sweep must still stop -- never
        # convert Ctrl-C / sys.exit into a soft-failed-but-continue cell.
        raise
    except BaseException as e:  # noqa: BLE001 -- no per-cell error may kill the batch
        ec = getattr(e, "exit_class", None) or "HARNESS_EXCEPTION"
        # a HarnessFault (Part A SDP/infra guard) carries a SPECIFIC reason token; keep it
        # structurally so the quarantine report names the real instrument failure rather
        # than just the unified HARNESS_EXCEPTION bucket.
        reason = getattr(e, "reason", None)
        # CELL_ERROR note + structured `error` field (main's observability, #42): the
        # crash type+message is preserved both in the greppable `error` field and in notes.
        return _failed_cell_row(task_spec, arm, seed, cfg, clock, ec,
                                f"{type(e).__name__}: {e}", backend,
                                harness_fault_reason=reason,
                                note_prefix="CELL_ERROR",
                                error=f"{type(e).__name__}: {e}")


def _iters_to_green(ep: EpisodeResult) -> int:
    # count loop iterations up to and including the green one
    count = 0
    for r in ep.per_iteration:
        count += 1
        if r.get("execute", {}).get("completed"):
            break
    return count


def _write_transcript(ws: str, state: LoopState, ep: EpisodeResult) -> str:
    path = os.path.join(ws, "transcript.json")
    with open(path, "w") as f:
        json.dump({
            "task": state.task, "seed": state.seed,
            "history": [dataclasses.asdict(p) for p in state.history],
            "per_iteration": ep.per_iteration,
            "feedback": state.feedback,
            "exit_class": ep.exit_class,
            "completed": ep.completed,
            # Part A.5: token usage in the persisted transcript too (per-iteration usage
            # lives in per_iteration[i]["tokens"]; these are the episode totals).
            "tokens": {"input": ep.input_tokens, "output": ep.output_tokens},
        }, f, indent=2)
    return path


# ---------------------------------------------------------------------------
# Backend factories
# ---------------------------------------------------------------------------
def make_replay_factories(trace_path: str):
    from harness.backends.replay import ReplayBackend, ReplayBrain, ReplayExecutor
    backend = ReplayBackend.from_file(trace_path)

    def make_brain(task, arm, seed):
        return ReplayBrain(backend.episode(task, arm.arm_id, seed))

    def make_executor(task, arm, seed):
        return ReplayExecutor(backend.episode(task, arm.arm_id, seed))

    return make_brain, make_executor


def assert_runtime_controls_match(cfg: StudyConfig, arms, tp_path: str) -> None:
    """Tie the identical-except-loop guarantee to the runtime sources (rigor).

    Refuses to run unless every arm's controlled fields match what the runner will
    ACTUALLY use: the shared cfg base model, and the shared prompt file behind each
    arm's task_prompt_ref. assert_identical_except_loop already proved the arms
    agree with each other; this proves they agree with the real config too.
    """
    problems = []
    cfg_prompt_base = os.path.basename(cfg.task_prompt_path)
    for arm_id, m in arms.items():
        if m.base_model_id != cfg.base_model_id:
            problems.append(
                f"arm {arm_id!r}.base_model_id={m.base_model_id!r} != cfg.base_model_id={cfg.base_model_id!r}")
        ref_path = m.task_prompt_ref.split("@", 1)[0]            # strip a @version tag
        if os.path.basename(ref_path) != cfg_prompt_base:
            problems.append(
                f"arm {arm_id!r}.task_prompt_ref={m.task_prompt_ref!r} does not point at the "
                f"runtime prompt {cfg.task_prompt_path!r}")
    if not os.path.exists(tp_path):
        problems.append(f"runtime task prompt file does not exist: {tp_path}")
    if problems:
        raise ValueError(
            "RUNTIME CONTROLS DO NOT MATCH THE ARMS (identical-except-loop confound):\n  "
            + "\n  ".join(problems) + "\nRefusing to run.")


def compose_task_prompt(preamble: str, task_spec: Dict[str, Any]) -> str:
    """Shared contract preamble + the per-task brief. The result is IDENTICAL
    across arms (neither piece is arm-dependent), so pre-reg §3 "same task prompt
    across arms" holds while each of the 15 tasks is fully specified."""
    brief = task_spec.get("prompt", "").strip()
    # NOTE: the internal engineering `title` (e.g. "...clean/dedup/enrich...") is
    # deliberately NOT injected here -- it would leak the fix/technique into the
    # agent-facing prompt and defeat the v3 ticket reframe (§1, prompt-no-leak).
    out = f"{preamble.strip()}\n\n## The ticket\n\n{brief}\n"
    # Multi-input tasks: name the ADDITIONAL inputs (locations only, never the fix
    # or a format/how-to hint). Identical across arms (task-, not arm-dependent) and
    # neutral by the prompt-no-leak guard. The concrete per-seed staged location of
    # each is listed with the dataset paths at run time.
    aux_names = [aux_input_name(a) for a in (task_spec.get("aux_inputs") or [])
                 if str(a).endswith(".py")]
    if aux_names:
        out += (
            "\n## Additional inputs\n\n"
            "Besides the primary dataset, this task also provides these named "
            f"inputs: {', '.join(aux_names)}. The location of each is listed with "
            "the dataset paths.\n")
    return out


def make_live_factories(cfg: StudyConfig, preamble: str, tasks_by_id: Dict[str, Any]):
    from harness.backends.live import AnthropicBrain, ConnectExecutor
    executor = ConnectExecutor(cfg.spark_remote, cfg.spark_rest_url)
    if not executor.reachable():
        raise RuntimeError(
            f"live backend selected but Spark Connect at {cfg.spark_remote} is not "
            "reachable. Bring up the Connect backend (catalog swap) first, or use "
            "--backend replay for offline validation."
        )
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("live backend selected but ANTHROPIC_API_KEY is not set.")

    def make_brain(task, arm, seed):
        prompt = compose_task_prompt(preamble, tasks_by_id[task])
        # send only the SAMPLING_SENT subset (one knob) -> no Claude 4.x 400.
        return AnthropicBrain(arm.base_model_id, prompt, sampling=sampling_kwargs(arm))

    def make_executor(task, arm, seed):
        # per-cell staging base under the cluster warehouse so each (task,arm,seed)
        # input lands at a deterministic, collision-free, cluster-reachable path.
        staging = None
        if cfg.warehouse_uri:
            staging = f"{cfg.warehouse_uri.rstrip('/')}/_ssa_staging/{task}/{arm.arm_id}/seed{seed}"
        return ConnectExecutor(cfg.spark_remote, cfg.spark_rest_url, staging_base=staging)

    return make_brain, make_executor


def _out_table_for(task_spec: Dict[str, Any], task: str) -> str:
    """The contract output table the executor reads back (mirrors run_cell)."""
    contract = task_spec.get("output_contract") or {}
    return contract.get("table") or f"agent_out_{task}"


def make_local_factories(cfg: StudyConfig, preamble: str, tasks_by_id: Dict[str, Any],
                         server, imperative_warehouse: str, imperative_ui_port: int):
    """Part-1 LOCAL substrate (DEVIATIONS D-7): isolate the imperative-vs-SDP
    paradigm by running each arm on its HOME local engine, off the remote EKS
    Connect substrate.

      * BRAIN: the SAME `AnthropicBrain` (claude-opus-4-8) the live path uses —
        UNCHANGED; needs `ANTHROPIC_API_KEY`.
      * EXECUTOR (routed PER PARADIGM): imperative arms (A, B2) run on classic
        in-process `local[*]` Spark via `LocalSparkExecutor` (classic Spark works
        there, so the imperative agents complete); SDP arms (B, B1) run on the LOCAL
        single-node Spark Connect server via `LocalConnectExecutor` (SDP requires a
        Connect server even locally), with its driver UI REST for the H2 stage-diff.

    H2 caveat (logged in D-7): the imperative executor-seconds (classic local Spark)
    and the SDP stage-diff (local Connect) are NOT directly comparable across the two
    engines; both are recorded alongside the always-present wall-clock cross-check.
    Part-1's clean cross-paradigm comparison is H1 / the silent-defect rate.
    """
    from harness.backends.live import AnthropicBrain
    from harness.backends.local import LocalSparkExecutor
    from harness.backends.local_connect import LocalConnectExecutor

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("local backend selected but ANTHROPIC_API_KEY is not set.")

    def make_brain(task, arm, seed):
        prompt = compose_task_prompt(preamble, tasks_by_id[task])
        # IDENTICAL to the live brain: same model, same one-knob sampling subset.
        return AnthropicBrain(arm.base_model_id, prompt, sampling=sampling_kwargs(arm))

    def make_executor(task, arm, seed):
        if arm.paradigm == "sdp":
            # SDP on the LOCAL single-node Connect server; H2 via its driver UI REST.
            return LocalConnectExecutor(server.remote, server.rest_url)
        # imperative on classic local[*] Spark; its own in-process UI port (distinct
        # from the Connect server's UI so the two stage/executor-seconds REST targets
        # never collide).
        out_table = _out_table_for(tasks_by_id[task], task)
        return LocalSparkExecutor(out_table=out_table, warehouse_dir=imperative_warehouse,
                                  ui_port=imperative_ui_port)

    return make_brain, make_executor


# ---------------------------------------------------------------------------
# Quarantine report (Part B.5): the excluded-data appendix for the paper.
# ---------------------------------------------------------------------------
def write_quarantine_report(path: str, tracker: "hf.HarnessFaultTracker",
                            backend: str, clock: float) -> Dict[str, Any]:
    """Write the QUARANTINE REPORT for every cell flagged HARNESS_ERROR: one record per
    excluded cell (task, seed, arm, exit_class, reason) plus the complexity bin, the live
    breaker counters, and the (tunable) breaker constants -- the reproducible record of
    what was excluded from H1-H4 and why. Always written (empty `cells` when nothing was
    quarantined). Returns the report dict."""
    report = {
        "generated_utc": now_utc_iso(clock),
        "backend": backend,
        "git_sha": git_sha(),
        "breaker_constants": {
            "global_harness_fault_limit": hf.GLOBAL_HARNESS_FAULT_LIMIT,
            "per_arm_harness_fault_limit": hf.PER_ARM_HARNESS_FAULT_LIMIT,
            "per_bin_harness_fault_limit": hf.PER_BIN_HARNESS_FAULT_LIMIT,
            "retry_delay_s": hf.HARNESS_FAULT_RETRY_DELAY_S,
        },
        "counters": tracker.counters(),
        "n_quarantined": len(tracker.records),
        "cells": [
            {"task": r.task, "seed": r.seed, "arm": r.arm,
             "exit_class": r.exit_class, "reason": r.reason,
             "complexity_bin": r.complexity_bin}
            for r in tracker.records
        ],
    }
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    return report


# ---------------------------------------------------------------------------
# Env metadata sidecar (pre-reg §8)
# ---------------------------------------------------------------------------
def write_env_sidecar(path: str, cfg: StudyConfig, arms: Dict[str, ArmManifest],
                      tasks_lock: Dict[str, Any], seeds_lock: Dict[str, Any],
                      backend: str, clock: float) -> None:
    import platform
    env = {
        "harness_version": HARNESS_VERSION,
        "schema_version": ResultRow.__dataclass_fields__["schema_version"].default,
        "generated_utc": now_utc_iso(clock),
        "git_sha": git_sha(),
        "spark_version": spark_version(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "base_model_id": cfg.base_model_id,
        "image_digest": cfg.image_digest,
        "executor_config": cfg.executor_config.to_dict(),
        "spark_remote": cfg.spark_remote,
        "backend": backend,
        "arms": {a: m.loop_signature() for a, m in arms.items()},
        "arm_shared_signature": next(iter(arms.values())).shared_signature() if arms else {},
        "tasks_lock_version": tasks_lock.get("version"),
        "n_tasks": len(tasks_lock.get("tasks", [])),
        "seeds_lock_version": seeds_lock.get("version"),
        "n_seeds": len(seeds_lock.get("seeds", [])),
    }
    with open(path, "w") as f:
        json.dump(env, f, indent=2)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv=None):
    here = os.path.dirname(os.path.abspath(__file__))
    study = os.path.dirname(here)
    ap = argparse.ArgumentParser(description="Multi-arm safe-agent study runner.")
    ap.add_argument("--config", default=os.path.join(study, "config", "study.config.json"))
    ap.add_argument("--arms-dir", default=os.path.join(study, "arms"))
    ap.add_argument("--tasks", default=os.path.join(study, "config", "TASKS.lock.json"))
    ap.add_argument("--seeds", default=os.path.join(study, "config", "SEEDS.lock.json"))
    ap.add_argument("--backend", choices=["replay", "live", "local"], default="replay")
    ap.add_argument("--replay-trace", default=None, help="episode trace JSON (replay backend)")
    # Part-1 LOCAL substrate (DEVIATIONS D-7): imperative arms on classic local[*]
    # Spark; SDP arms on a LOCAL single-node Spark Connect server this runner brings
    # up (and tears down). These knobs size that local Connect server.
    ap.add_argument("--local-connect-port", type=int, default=15002,
                    help="gRPC port for the local Spark Connect server (--backend local)")
    ap.add_argument("--local-ui-port", type=int, default=4040,
                    help="driver UI port for the local Connect server's H2 stage-diff "
                         "(--backend local); the imperative LocalSparkExecutor uses +1")
    ap.add_argument("--out", default=os.path.join(study, "results", "results.jsonl"))
    ap.add_argument("--work-dir", default=os.path.join(study, ".work"))
    ap.add_argument("--only-tasks", default=None, help="comma-separated task ids to restrict to")
    ap.add_argument("--only-arms", default=None, help="comma-separated arm ids to restrict to")
    ap.add_argument("--max-seeds", type=int, default=None, help="cap seeds (e.g. 1 for smoke)")
    ap.add_argument("--per-cell-timeout", type=float, default=PER_CELL_WALLCLOCK_TIMEOUT_S,
                    help="HARD per-cell wall-clock cap (s): a (task,arm,seed) cell exceeding "
                         "it is abandoned as a harness_error timeout row (in-process Spark "
                         "JVM force-killed) and the sweep advances. Backstop against a wedged "
                         "cell; set well above the per-iteration exec budget.")
    ap.add_argument("--clock", type=float, default=None,
                    help="UTC epoch seconds to stamp rows (default: time.time()); fixed for reproducible tests")
    args = ap.parse_args(argv)

    clock = args.clock if args.clock is not None else time.time()
    # Part A.1: canonicalize the work dir to an ABSOLUTE path ONCE, here. Every per-cell
    # workspace (`ws = os.path.join(work_dir, ...)`) and thus `state.workspace` inherits
    # it, so the SDP CLI's `--spec` and subprocess cwd are absolute and can never form
    # the doubled relative path that produced PIPELINE_SPEC_FILE_DOES_NOT_EXIST.
    args.work_dir = os.path.abspath(args.work_dir)
    cfg = StudyConfig.from_file(args.config)
    arms = load_arms(args.arms_dir)            # asserts arms identical-except-loop
    # the task prompt path in the config may be repo-relative
    tp_path = cfg.task_prompt_path
    if not os.path.isabs(tp_path):
        tp_path = os.path.join(study, tp_path)
    with open(tp_path) as f:
        task_prompt = f.read()

    # RIGOR: tie the identical-except-loop guarantee to the ACTUAL runtime sources,
    # not just to agreement among the manifests. Every arm's base_model_id must
    # equal the shared cfg model that the runner will actually use, and every arm's
    # task_prompt_ref must point at the shared prompt file the runner actually
    # loads. Otherwise an arm could agree with its peers yet diverge from the real
    # config -- a silent confound. Refuse to run if not.
    assert_runtime_controls_match(cfg, arms, tp_path)

    with open(args.tasks) as f:
        tasks_lock = json.load(f)
    with open(args.seeds) as f:
        seeds_lock = json.load(f)

    tasks = tasks_lock["tasks"]
    tasks_by_id = {t["id"]: t for t in tasks}
    seeds = list(seeds_lock["seeds"])
    if args.max_seeds:
        seeds = seeds[: args.max_seeds]

    only_tasks = set(args.only_tasks.split(",")) if args.only_tasks else None
    only_arms = set(args.only_arms.split(",")) if args.only_arms else None

    os.makedirs(args.work_dir, exist_ok=True)
    local_server = None  # set for --backend local; torn down in the finally
    n = 0
    # ONE try whose finally tears down the local Connect JVM on ANY subsequent
    # failure. The server is created + started INSIDE this try (not before it), so a
    # post-launch error -- ensure_schema failing, make_local_factories raising on a
    # missing API key, or the run loop itself -- can never leak the JVM (the finally
    # is always reachable once `local_server` exists). stop() is a no-op if the JVM
    # was never spawned.
    try:
        if args.backend == "replay":
            if not args.replay_trace:
                ap.error("--backend replay requires --replay-trace")
            make_brain, make_executor = make_replay_factories(args.replay_trace)
            backend_name = "replay"
        elif args.backend == "local":
            # Part-1 LOCAL substrate (D-7): split engines per paradigm. We bring up ONE
            # single-node Spark Connect server for the SDP arms, point the SDP storage +
            # remote at it, and ensure the spark_catalog.default schema the SDP CLI needs
            # exists BEFORE any dry-run/run. The imperative arms run on classic local[*]
            # Spark (LocalSparkExecutor) and never touch this server.
            from harness.backends.local_connect import LocalConnectServer
            localwh = os.path.abspath(os.path.join(args.work_dir, "_localwh"))
            os.makedirs(localwh, exist_ok=True)
            local_server = LocalConnectServer(
                port=args.local_connect_port, ui_port=args.local_ui_port,
                warehouse_dir=localwh,
                log_file=os.path.join(args.work_dir, "_local_connect.log"))
            local_server.start(ensure_catalog=cfg.sdp_catalog, ensure_database=cfg.sdp_database)
            # local file:// for BOTH engines: SDP spec storage lands under the local
            # warehouse (per task/arm/seed via _sdp_storage_for); the SDP CLI dials the
            # local Connect server; H2 stage-diff reads its driver UI REST.
            cfg.warehouse_uri = f"file://{localwh}"
            cfg.spark_remote = local_server.remote
            cfg.spark_rest_url = local_server.rest_url
            make_brain, make_executor = make_local_factories(
                cfg, task_prompt, tasks_by_id, local_server,
                imperative_warehouse=os.path.join(localwh, "imperative"),
                imperative_ui_port=args.local_ui_port + 1)
            backend_name = "local"
        else:
            make_brain, make_executor = make_live_factories(cfg, task_prompt, tasks_by_id)
            backend_name = "live"

        # HARNESS-FAULT POLICY (Part B): the per-cell net keeps the batch alive; this
        # tracker adds the retry-once -> quarantine -> circuit-breaker layer ON TOP, so a
        # broken INSTRUMENT (SDP/infra fault from Part A OR a propose fault from #31) can
        # never masquerade as an agent result and a SYSTEMIC instrument failure aborts the
        # run loudly instead of silently polluting the stats.
        tracker = hf.HarnessFaultTracker()

        def _cleanup() -> None:
            rep = hf.hard_reset_after_fault(local_server)
            if rep.get("notes"):
                print(f"[runner] hard-reset after harness fault: {rep}", file=sys.stderr)

        quarantine_path = args.out.replace(".jsonl", ".quarantine.json")
        try:
            with open(args.out, "w") as out:
                for t in tasks:
                    task_id = t["id"]
                    if only_tasks and task_id not in only_tasks:
                        continue
                    cbin = hf.task_complexity_bin(t)
                    for arm_id, arm in arms.items():
                        if only_arms and arm_id not in only_arms:
                            continue
                        for seed in seeds:
                            # CIRCUIT BREAKER: BEFORE starting this cell, abort the whole
                            # run LOUDLY if a prior quarantine breached any threshold.
                            tracker.check_breaker()
                            # (1) retry-once -> (2) quarantine+continue, with (4) per-cell
                            # hard reset after any fault. The run_fn is `run_cell_guarded`:
                            # main's HARD per-cell wall-clock deadline + in-process-JVM
                            # force-kill backstop (the live-calibration wedge fix), which
                            # itself runs the cell through `_run_cell_safe` (the inner net
                            # that classifies any cell-level error into a soft-failed row).
                            # So a single bad cell never raises, a WEDGE is bounded+killed,
                            # and process_cell adds the instrument-validity layer on top.
                            row, qreason = hf.process_cell(
                                (lambda t=t, arm=arm, seed=seed: run_cell_guarded(
                                    t, arm, seed, cfg, make_brain, make_executor,
                                    args.work_dir, clock,
                                    per_cell_timeout_s=args.per_cell_timeout,
                                    backend_name=backend_name)),
                                cleanup=_cleanup)
                            out.write(row.to_json() + "\n")
                            out.flush()  # persist progress per cell (survives a later crash)
                            n += 1
                            if qreason is not None:
                                # (2) excluded from H1-H4; recorded for the breaker + report.
                                tracker.record_quarantine(task_id, seed, arm_id, qreason, cbin)
                                print(f"[runner] QUARANTINED {row.run_id}: HARNESS_ERROR "
                                      f"(underlying {qreason}); EXCLUDED from H1-H4. "
                                      f"counters={tracker.counters()}", file=sys.stderr)
                                continue
                            # executor_seconds is the MEASURED surface (None if no live metric);
                            # show the wall-clock cross-check value when unmeasured.
                            exec_s = (f"{row.executor_seconds:.1f}" if row.executor_seconds is not None
                                      else f"~{row.executor_seconds_wallclock:.1f}(wall)")
                            print(f"[runner] {row.run_id}: silent_defect={row.silent_defect} "
                                  f"exit={row.exit_class} exec_s={exec_s} "
                                  f"usd=${row.usd:.4f}", file=sys.stderr)

            # final breaker check: a breach on the LAST cell (no next cell to gate) must
            # still abort the run loudly rather than let it report as clean.
            tracker.check_breaker()
            write_env_sidecar(args.out.replace(".jsonl", ".env.json"), cfg, arms,
                              tasks_lock, seeds_lock, backend_name, clock)
            print(f"[runner] wrote {n} rows to {args.out}", file=sys.stderr)
        except hf.CircuitBreakerTripped as e:
            # LOUD abort: surface the breach + counters, then re-raise so the run stops
            # (deliberate, like Ctrl-C). The quarantine report is still written below.
            print(f"\n[runner] !!! {e}\n", file=sys.stderr)
            raise
        finally:
            # ALWAYS emit the quarantine report (even on a breaker abort) so the paper's
            # excluded-data appendix is reproducible from this run.
            write_quarantine_report(quarantine_path, tracker, backend_name, clock)
            if tracker.records:
                print(f"[runner] {len(tracker.records)} cell(s) QUARANTINED "
                      f"(HARNESS_ERROR); report -> {quarantine_path}", file=sys.stderr)
    finally:
        if local_server is not None:
            local_server.stop()


if __name__ == "__main__":
    main()
