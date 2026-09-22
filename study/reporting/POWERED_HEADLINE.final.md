# Safe-agent study — headline numbers

- rows: **528**  |  (task,seed) cells: **264**  |  arms: ['A', 'B']
- CI: bootstrap **percentile**, B=**10000**, seed=**20260623**, resample unit=**(task, seed)**
- inference: mixed-effects logistic (random intercepts task+seed); Holm across 5 contrasts; α=0.05

## H1 — silent-defect rate by arm

| arm | silent-defect rate | k/n | 95% CI (bootstrap) |
|---|---|---|---|
| A | 0.277 | 73/264 | [0.223, 0.330] |
| B | 0.326 | 86/264 | [0.269, 0.383] |

## H1 — paired contrasts (Holm over the **glmm** contrast p-values)

| contrast | Δ rate | 95% CI (bootstrap) | OR | p (source) | Holm p | sig (α=.05) |
|---|---|---|---|---|---|---|
| A-B | -0.049 | [-0.098, +0.000] | 1.974 | 0.0033 | 0.0033 | YES |

## H1 — GLMM odds ratios + average marginal effects (primary inference)

Reference = Arm A. OR<1 ⇒ FEWER silent defects than A. AME is the model-based average marginal effect on P(silent_defect) (RE at 0).

| arm vs A | odds ratio | coef | AME (Δ prob) | posterior p |
|---|---|---|---|---|
| B | 1.974 | +0.680 | +0.080 | 0.0033 |

Reference = Arm B (B-vs-B1 / B-vs-B2 ablation contrasts):

| arm vs B | odds ratio | coef | AME (Δ prob) | posterior p |
|---|---|---|---|---|

Observed per-arm silent-defect rates (DESCRIPTIVE, not the AME):
  A=0.277, B=0.326

## H1 — power / sample-size rule (pre-reg §6)

- target: 95% CI half-width ≤ **0.05** on A−B
- observed sd(A−B paired) = **0.411**  →  required N = **260** (task,seed) cells; have **264**
- **meets power: True** — `headline_n_valid=True` (a headline N below required must NOT be claimed)


## §9 — Error taxonomy: catch-stage by class group (PRE-REGISTERED headline)

Where each defect is caught: **dry_run** (SDP gate, pre-execution), **runtime** (during execute), or **never** (shipped). Counted at the defect level across ALL iterations (anti-bypass §9.2 — a gate-caught-then-fixed error still counts). structural=D1/D4/D5, semantic/silent=D2/D6/D7/D8 (CONTROL), state=D3/D9. The silent-defect rate (H1 above) is the control, not the headline.

| arm | group | dry_run (gate) | runtime | never (shipped) |
|---|---|---|---|---|
| A | structural | 0 | 4 | 0 |
| A | semantic | 0 | 0 | 90 |
| A | state | 0 | 0 | 0 |
| B | structural | 79 | 30 | 0 |
| B | semantic | 0 | 0 | 106 |
| B | state | 0 | 0 | 0 |

Iteration-level error events per arm: **A** gate=0 runtime=193 intercepts=0; **B** gate=349 runtime=155 intercepts=353

## H2 — compute-to-correct: A (execute-only) vs gated loop

- metric: **`executor_seconds_wallclock_to_correct`** — uniform wall-clock executor-seconds proxy (cross-arm comparable)
- selection: backend='local': local substrate measured field is not cross-arm comparable

Reported BOTH ways (B9): intention-to-treat over all matched cells (failed runs imputed to total compute spent) AND complete-case (both arms reached correct) as the pre-specified sensitivity.

| gated arm | mode | n pairs | median exec-s saved | mean | 95% CI | $ saved | intercept frac |
|---|---|---|---|---|---|---|---|
| B | intention_to_treat | 264 | 9.1 | -12.0 | [-23.8, -2.9] | $0.34 | 69.5% (353/508) |
| B | complete_case | 251 | 9.1 | -3.5 | [-9.5, 1.6] | $0.32 | 69.5% (353/508) |

## H4/H5 — conciseness: declarative (B) vs imperative (A)

Paired over (task, seed) on the FINAL ACCEPTED program. Contrast **B-vs-A** (locked 2-arm design; A2 withdrawn). Difference is **A − B**, so positive ⇒ the declarative agent wrote LESS. LOC/AST are source-level (substrate-independent). `*_body` excludes the mandatory @dp/def/import scaffolding (the SDP `spark-pipeline.yml` is harness boilerplate and is not counted).

| metric | n pairs | B (declarative) | A (imperative) | Δ (A−B) | 95% CI (bootstrap) | % smaller than imperative |
|---|---|---|---|---|---|---|
| final_program_loc | 251 | 67.9 | 134.0 | +66.1 | [+61.9, +70.4] | 49.3% |
| final_program_loc_body | 251 | 61.1 | 126.9 | +65.8 | [+61.9, +69.8] | 51.9% |
| ast_node_count | 251 | 614.9 | 1105.5 | +490.6 | [+453.2, +530.4] | 44.4% |
| ast_node_count_body | 251 | 594.4 | 1075.6 | +481.1 | [+444.6, +519.5] | 44.7% |

## Quarantine — HARNESS_ERROR cells EXCLUDED from H1–H4 (excluded-data appendix)

- rows analyzed: **528** of **528** total; **0** quarantined (instrument failures, never agent outcomes)
- no cells quarantined — every analyzed cell is a genuine agent outcome.

