# Safe Agentic Spark Development — benchmark harness

Research-grade instrument for the pre-registered study in
[`PREREGISTRATION.md`](PREREGISTRATION.md). It measures whether constraining an
AI coding agent's Spark loop with **SDP + a safety skill + a structural dry-run
gate** (Arm B) ships **fewer silent data-correctness defects** than an
unconstrained imperative-PySpark agent (Arm A), and whether the gate **saves
compute** — with the two ablations (B1 = SDP-only, B2 = gate-only) that turn
"B is better" into "*here is which component does the work*".

> **Status: powered run complete** (Section 1 is the reported study; results in
> `results/`, headlines in `reporting/`). See `DEVIATIONS.md` for the as-run record.

## Layout

```
safe_agent_study/
  PREREGISTRATION.md     the binding design (hypotheses, arms, metrics, analysis)
  DEVIATIONS.md          D-0 RESOLVED (corpus 6->15) + impl notes
  config/
    TASKS.lock.json      frozen 22-task corpus (3 substrates) + balanced defects
    SEEDS.lock.json      fixed integer seeds
    study.config.json    SHARED controlled config (model, prompt path, cluster/$)
    results_schema.json  machine-readable results.jsonl row contract
  results/               committed result files + env/quarantine sidecars behind every number
  reporting/             POWERED / SUPPLEMENTAL headline + report artifacts
  prompts/task_prompt.md the ONE task prompt every arm receives
  arms/{A,B,B1,B2}.json   arm manifests (only the LOOP fields differ)
  harness/
    schema.py            frozen ResultRow + env sidecar
    cost.py              executor-seconds + USD + dry-run interception (H2); ITT compute-to-correct
    oracles.py           arm-BLIND grader (RunOutcome carries no gate-revealing field)
    output_oracles.py    grade the agent's MATERIALIZED table vs ground truth (live path, B1)
    arm_manifest.py      manifests + identical-except-loop guard (pre-reg §3)
    runner.py            multi-arm loop, writes agent code, builds profile from real table, blind grade
    run_battery.sh       parameterized E3 battery (--seeds/--N/--defects/--arms/--trials)
    backends/            pluggable brain+executor:
      replay.py            offline replay (plumbing tests)
      live.py              anthropic brain + Connect executor (the sweep; delta executor-seconds)
      local.py             scripted brain + in-process real-Spark executor (live-path validation)
  analysis/
    analyze.py           bootstrap CIs + GLMM + Holm + H2  ->  headline table
    requirements.txt
  tests/                 offline validation (grader vs E3/pilots; runner; stats)
```

The E3 defect battery it builds on lives at `../defect_battery/` (variants +
`quantify.py` + `quantify_ext.py` + `run_battery.sh`). `quantify.py` (orders) and
`quantify_ext.py` (CDC + payments) are the **single source of oracle truth**;
`oracles.py` and the tests import them rather than re-implementing them.

### Task corpus (15 tasks, 3 substrates)

`config/TASKS.lock.json` freezes 22 realistic pipelines over three independent data
substrates, so each semantic defect class is corroborated on more than one
dataset (not a single-dataset artifact):

- **orders** (`generators/gen_messy_orders.py`, ref seed 42) — D2/D6/D7/D8 via `quantify.py`
- **customer-CDC** (`generators/gen_customers_cdc.py`, ref seed 7) — D6 via `quantify_ext.cdc_d6`
- **multi-currency payments** (`generators/gen_payments.py`, ref seed 42) — D7/D8 via `quantify_ext.pay_d7|pay_d8`

Every D1–D9 class is exhibited by ≥5 tasks (coverage matrix asserted by
`tests/test_corpus.py`). Each task carries a per-task `prompt` (the engineering
brief, identical across arms — pre-reg §3) composed with the shared preamble.

## How the four arms are kept identical-except-loop (validity crux, pre-reg §3)

The only manipulated variable is the development loop. We make that **auditable**,
not aspirational:

- The **controlled** variables — base model, task prompt text, sampling params,
  max iterations, and the per-seed input data — come from ONE shared place
  (`config/study.config.json` + `prompts/task_prompt.md`), and each arm manifest repeats
  them verbatim.
- `arm_manifest.assert_identical_except_loop()` runs at load time and **refuses
  to start** if any two arms differ on a controlled field. Only these LOOP fields
  may vary: `paradigm`, `dry_run_gate`, `safety_skill`, `skills`,
  `allowed_commands`. (`tests/test_runner_offline.py::test_identical_except_loop_guard_fires`
  perturbs `base_model_id` and asserts the guard raises.)
- The **runner owns the loop, the cost model, and the grading** — not the
  backend — so no backend can confound the comparison. The same per-seed dataset
  (`gen_messy_orders.py --seed <seed>`) is generated once and shared across arms,
  so a matched seed means byte-identical input.

## How the grader stays blind (pre-reg §5)

`oracles.grade_run(spec, outcome)` takes **no `arm` argument**, and `RunOutcome`
carries no arm/model/skill/gate fields — only neutral observations (did it
complete? what's in the output? what error class appeared, at which stage?).
`tests/test_oracles.py::test_grader_is_blind_to_arm` asserts the signature and
the dataclass never leak the arm. The numeric oracles ARE the E3 quantifiers
(imported), so the grader cannot drift from the registered numbers.

## Cost accounting (H2)

Per iteration (`harness/cost.py`):
- **dry-run gate** iteration = driver-only structural analysis → **0
  executor-seconds, $0** (the compute a doomed run would have wasted, avoided).
- **execute** iteration = ran on the cluster → executor-seconds from the Spark
  REST API (`/applications/<id>/executors` `totalDuration`), or a declared
  `wall_s × instances` fallback; USD = `executor_seconds/3600 × price/executor-hr`
  at the price in `config/study.config.json` (carried into every row).

`compute-to-correct` sums executor-seconds up to and including the first green
iteration; the **dry-run intercept fraction** = failing iterations caught at the
gate ÷ all failing iterations. Both Arm A (execute-only) and the gated arms are
instrumented.

## Reproduce the offline validation

```bash
cd experiments/safe_agent_study

# 1. grader vs the registered E3 numbers + the n=1 pilots (needs pyspark)
python3 tests/test_oracles.py
#   -> E3 numbers reproduced: D2=246 D6=0 D7=275 D8=250/$49778.06 ; 9/9 passed

# 1b. extended quantifiers (CDC + payments substrates), fixed-seed numbers
python3 tests/test_oracles_ext.py
#   -> cdc_d6=77 ; pay_d7=935 ; pay_d8=1139/$371010.60 ; 4/4 passed

# 1c. corpus integrity (15 tasks, coverage matrix, quantifier resolution) — no Spark
python3 tests/test_corpus.py                    # 8/8 passed

# 2. runner end-to-end offline (replay backend) + cost + schema + stats plumbing
python3 tests/test_runner_offline.py            # 5/5 passed

# 2b. LIVE measurement path on REAL Spark: write code -> execute -> materialize ->
#     read back -> output oracle -> grade. Proves silent_defect TRUE on a
#     completed-but-defective output, FALSE on correct; delta executor-seconds.
python3 tests/test_live_path.py
#   -> LIVE GRADE correct: silent=False ; d8: silent=True [D8] ; d2: silent=True [D2] ;
#      completion-check: missing output table -> OUTPUT_TABLE_NOT_FOUND ; 3/3 passed

# 3. the parameterized E3 battery (real SDP dry-run; needs a JDK)
bash harness/run_battery.sh --seeds 42 --defects D1,D2,D8 --arms sdp,plain

# 4. the analysis on any results.jsonl (GLMM needs analysis/requirements.txt)
python3 analysis/analyze.py results.jsonl --env results.env.json --md-out HEADLINE.md
```

## Run the sweep (AFTER the GO + Connect backend is up)

```bash
# offline dry-run of the whole pipeline with a replay trace:
python3 harness/runner.py --backend replay --replay-trace tests/fixtures/pilot_episodes.json

# the real sweep (needs ANTHROPIC_API_KEY + a reachable sc:// backend):
python3 harness/runner.py --backend live --config config/study.config.json \
        --arms-dir arms --tasks config/TASKS.lock.json --seeds config/SEEDS.lock.json \
        --out results.jsonl
python3 analysis/analyze.py results.jsonl --env results.env.json --md-out HEADLINE.md

# Part-1 LOCAL substrate (needs ANTHROPIC_API_KEY + a JDK; no remote cluster — see
# DEVIATIONS D-7). Isolates the imperative-vs-SDP paradigm: imperative arms (A, B2)
# run on classic local[*] Spark; SDP arms (B, B1) on a local single-node Spark
# Connect server the runner starts and stops itself. Local file:// for both engines.
python3 harness/runner.py --backend local --config config/study.config.json \
        --arms-dir arms --tasks config/TASKS.lock.json --seeds config/SEEDS.lock.json \
        --out results_part1.jsonl
#   --local-connect-port (default 15002) / --local-ui-port (default 4040; the
#   imperative LocalSparkExecutor UI uses +1) size the local Connect server.
```

Every `results.jsonl` row records Spark version, image digest, git SHA, base-model
id, seed, executor config, iterations, wall/executor-seconds, USD, exit class,
`silent_defect`, `defect_classes[]`, and `detection_stage` (pre-reg §8); a
`results.env.json` sidecar captures the run environment.
