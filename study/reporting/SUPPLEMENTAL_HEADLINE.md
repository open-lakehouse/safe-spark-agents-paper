# Safe-agent study — headline numbers

- rows: **24**  |  (task,seed) cells: **12**  |  arms: ['A', 'B']
- CI: bootstrap **percentile**, B=**10000**, seed=**20260623**, resample unit=**(task, seed)**
- inference: mixed-effects logistic (random intercepts task+seed); Holm across 5 contrasts; α=0.05

## H1 — silent-defect rate by arm

| arm | silent-defect rate | k/n | 95% CI (bootstrap) |
|---|---|---|---|
| A | 1.000 | 12/12 | [1.000, 1.000] |
| B | 1.000 | 12/12 | [1.000, 1.000] |

## H1 — paired contrasts (Holm over the **mcnemar_fallback** contrast p-values)

_GLMM unavailable; Holm applied to the **McNemar fallback** p-values (labelled). Install statsmodels for the pre-registered GLMM inference._

| contrast | Δ rate | 95% CI (bootstrap) | OR | p (source) | Holm p | sig (α=.05) |
|---|---|---|---|---|---|---|
| A-B | +0.000 | [+0.000, +0.000] | — | 1.0000 | 1.0000 | no |

## H1 — GLMM odds ratios + average marginal effects (primary inference)

_GLMM not fitted: GLMM not identifiable: silent_defect has no variation (all 1); report rates/CIs only._

Observed silent-defect rate per arm (descriptive):

| arm | rate |
|---|---|
| A | 1.000 |
| B | 1.000 |

## H1 — power / sample-size rule (pre-reg §6)

- target: 95% CI half-width ≤ **0.05** on A−B
- observed sd(A−B paired) = **0.000**  →  required N = **1** (task,seed) cells; have **12**
- **meets power: True** — `headline_n_valid=True` (a headline N below required must NOT be claimed)


## §9 — Error taxonomy: catch-stage by class group (PRE-REGISTERED headline)

Where each defect is caught: **dry_run** (SDP gate, pre-execution), **runtime** (during execute), or **never** (shipped). Counted at the defect level across ALL iterations (anti-bypass §9.2 — a gate-caught-then-fixed error still counts). structural=D1/D4/D5, semantic/silent=D2/D6/D7/D8 (CONTROL), state=D3/D9. The silent-defect rate (H1 above) is the control, not the headline.

| arm | group | dry_run (gate) | runtime | never (shipped) |
|---|---|---|---|---|
| A | structural | 0 | 0 | 0 |
| A | semantic | 0 | 0 | 12 |
| A | state | 0 | 0 | 0 |
| B | structural | 0 | 0 | 0 |
| B | semantic | 0 | 0 | 14 |
| B | state | 0 | 0 | 0 |

Iteration-level error events per arm: **A** gate=0 runtime=4 intercepts=0; **B** gate=12 runtime=4 intercepts=13

## H2 — compute-to-correct: A (execute-only) vs gated loop

- metric: **`executor_seconds_wallclock_to_correct`** — uniform wall-clock executor-seconds proxy (cross-arm comparable)
- selection: backend='local': local substrate measured field is not cross-arm comparable

Reported BOTH ways (B9): intention-to-treat over all matched cells (failed runs imputed to total compute spent) AND complete-case (both arms reached correct) as the pre-specified sensitivity.

| gated arm | mode | n pairs | median exec-s saved | mean | 95% CI | $ saved | intercept frac |
|---|---|---|---|---|---|---|---|
| B | intention_to_treat | 12 | 11.0 | -14.6 | [-36.9, 6.2] | $0.02 | 76.5% (13/17) |
| B | complete_case | 12 | 11.0 | -14.6 | [-36.4, 6.2] | $0.02 | 76.5% (13/17) |

## H4/H5 — conciseness: declarative (B) vs imperative (A)

Paired over (task, seed) on the FINAL ACCEPTED program. Contrast **B-vs-A** (locked 2-arm design; A2 withdrawn). Difference is **A − B**, so positive ⇒ the declarative agent wrote LESS. LOC/AST are source-level (substrate-independent). `*_body` excludes the mandatory @dp/def/import scaffolding (the SDP `spark-pipeline.yml` is harness boilerplate and is not counted).

| metric | n pairs | B (declarative) | A (imperative) | Δ (A−B) | 95% CI (bootstrap) | % smaller than imperative |
|---|---|---|---|---|---|---|
| final_program_loc | 12 | 108.9 | 184.5 | +75.6 | [+50.8, +106.2] | 41.0% |
| final_program_loc_body | 12 | 100.4 | 175.7 | +75.2 | [+52.1, +103.9] | 42.8% |
| ast_node_count | 12 | 946.6 | 1654.2 | +707.7 | [+549.5, +894.2] | 42.8% |
| ast_node_count_body | 12 | 925.9 | 1629.3 | +703.4 | [+546.7, +878.8] | 43.2% |

## Quarantine — HARNESS_ERROR cells EXCLUDED from H1–H4 (excluded-data appendix)

- rows analyzed: **24** of **24** total; **0** quarantined (instrument failures, never agent outcomes)
- no cells quarantined — every analyzed cell is a genuine agent outcome.

