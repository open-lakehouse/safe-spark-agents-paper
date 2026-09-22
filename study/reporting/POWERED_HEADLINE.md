# Safe-agent study — headline numbers

- rows: **518**  |  (task,seed) cells: **264**  |  arms: ['A', 'B']
- CI: bootstrap **percentile**, B=**10000**, seed=**20260623**, resample unit=**(task, seed)**
- inference: mixed-effects logistic (random intercepts task+seed); Holm across 5 contrasts; α=0.05

## H1 — silent-defect rate by arm

| arm | silent-defect rate | k/n | 95% CI (bootstrap) |
|---|---|---|---|
| A | 0.277 | 73/264 | [0.223, 0.330] |
| B | 0.335 | 85/254 | [0.276, 0.394] |

## H1 — paired contrasts (Holm over the **glmm** contrast p-values)

| contrast | Δ rate | 95% CI (bootstrap) | OR | p (source) | Holm p | sig (α=.05) |
|---|---|---|---|---|---|---|
| A-B | -0.047 | [-0.098, +0.004] | 1.849 | 0.0072 | 0.0072 | YES |

## H1 — GLMM odds ratios + average marginal effects (primary inference)

Reference = Arm A. OR<1 ⇒ FEWER silent defects than A. AME is the model-based average marginal effect on P(silent_defect) (RE at 0).

| arm vs A | odds ratio | coef | AME (Δ prob) | posterior p |
|---|---|---|---|---|
| B | 1.849 | +0.615 | +0.071 | 0.0072 |

Reference = Arm B (B-vs-B1 / B-vs-B2 ablation contrasts):

| arm vs B | odds ratio | coef | AME (Δ prob) | posterior p |
|---|---|---|---|---|

Observed per-arm silent-defect rates (DESCRIPTIVE, not the AME):
  A=0.277, B=0.335

## H1 — power / sample-size rule (pre-reg §6)

- target: 95% CI half-width ≤ **0.05** on A−B
- observed sd(A−B paired) = **0.424**  →  required N = **276** (task,seed) cells; have **254**
- **meets power: False** — `headline_n_valid=False` (a headline N below required must NOT be claimed)


## §9 — Error taxonomy: catch-stage by class group (PRE-REGISTERED headline)

Where each defect is caught: **dry_run** (SDP gate, pre-execution), **runtime** (during execute), or **never** (shipped). Counted at the defect level across ALL iterations (anti-bypass §9.2 — a gate-caught-then-fixed error still counts). structural=D1/D4/D5, semantic/silent=D2/D6/D7/D8 (CONTROL), state=D3/D9. The silent-defect rate (H1 above) is the control, not the headline.

| arm | group | dry_run (gate) | runtime | never (shipped) |
|---|---|---|---|---|
| A | structural | 0 | 4 | 0 |
| A | semantic | 0 | 0 | 90 |
| A | state | 0 | 0 | 0 |
| B | structural | 78 | 29 | 0 |
| B | semantic | 0 | 0 | 105 |
| B | state | 0 | 0 | 0 |

Iteration-level error events per arm: **A** gate=0 runtime=193 intercepts=0; **B** gate=342 runtime=153 intercepts=346

## H2 — compute-to-correct: A (execute-only) vs gated loop

- metric: **`executor_seconds_wallclock_to_correct`** — uniform wall-clock executor-seconds proxy (cross-arm comparable)
- selection: backend='local': local substrate measured field is not cross-arm comparable

Reported BOTH ways (B9): intention-to-treat over all matched cells (failed runs imputed to total compute spent) AND complete-case (both arms reached correct) as the pre-specified sensitivity.

| gated arm | mode | n pairs | median exec-s saved | mean | 95% CI | $ saved | intercept frac |
|---|---|---|---|---|---|---|---|
| B | intention_to_treat | 252 | 9.1 | -13.0 | [-24.9, -3.3] | $0.32 | 69.3% (346/499) |
| B | complete_case | 239 | 9.1 | -4.1 | [-10.5, 1.2] | $0.30 | 69.3% (346/499) |

## H4/H5 — conciseness: declarative (B) vs imperative (A)

Paired over (task, seed) on the FINAL ACCEPTED program. Contrast **B-vs-A** (locked 2-arm design; A2 withdrawn). Difference is **A − B**, so positive ⇒ the declarative agent wrote LESS. LOC/AST are source-level (substrate-independent). `*_body` excludes the mandatory @dp/def/import scaffolding (the SDP `spark-pipeline.yml` is harness boilerplate and is not counted).

| metric | n pairs | B (declarative) | A (imperative) | Δ (A−B) | 95% CI (bootstrap) | % smaller than imperative |
|---|---|---|---|---|---|---|
| final_program_loc | 239 | 64.9 | 131.2 | +66.2 | [+61.9, +70.7] | 50.5% |
| final_program_loc_body | 239 | 58.3 | 124.1 | +65.8 | [+61.7, +70.0] | 53.0% |
| ast_node_count | 239 | 577.4 | 1075.1 | +497.7 | [+459.8, +537.9] | 46.3% |
| ast_node_count_body | 239 | 557.9 | 1044.8 | +486.9 | [+449.4, +527.3] | 46.6% |

## Quarantine — HARNESS_ERROR cells EXCLUDED from H1–H4 (excluded-data appendix)

- rows analyzed: **518** of **520** total; **2** quarantined (instrument failures, never agent outcomes)
- by arm: {'B': 2}  |  by reason: {'PROPOSE_API_ERROR': 2}

| task | seed | arm | exit_class | reason |
|---|---|---|---|---|
| HC2_session_funnel | 42 | B | HARNESS_ERROR | PROPOSE_API_ERROR |
| HC2_session_funnel | 3141 | B | HARNESS_ERROR | PROPOSE_API_ERROR |
