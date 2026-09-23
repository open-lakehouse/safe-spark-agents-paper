# Section 1 Supplemental Materials

*Companion to the paper [Safe Governed AI Data Engineering on Spark](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/paper/PAPER.md). This is Section 1's methods appendix, detailed forensics, the pre-registered run protocol, operational definitions, and full materials, split out of the main paper to keep it readable. The paper cites these sections inline as §SM1–§SM7.*

---

## Supplemental Materials (Section 1)

> **Reading for the results? Skip this block, jump to Section 2.** What follows is Section 1's methods appendix, detailed forensics, the pre-registered protocol, operational definitions, and full materials, retained for reproduction and deep review, not the paper's through-line. The main text cites it inline as §SM1, §SM2, §SM3, §SM6, §SM7.

## SM1. Root-cause forensics: the D7 timezone skill gap (full detail)

*Expanded from §SM1. The main text gives the resolved result (D7 7→0; parity once arm B is taught the UTC idiom). This is the underlying mechanism, the three-agent code audit, the parallel D8 analysis, the validated skill-swap, and the remediations for framework and skill owners.*

**D7 (timezone): the immutable-config safety property removes the fix imperative uses.** The executor box runs `America/Los_Angeles`, and the harness deliberately does **not** pin session tz in the SDP manifest (pinning it would hand SDP correct-UTC "for free," an asymmetric advantage [[`runner.py:418-422`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/runner.py#L418-L422)]) so the default session tz is Pacific for *both* arms. Imperative (A) owns its `SparkSession` and sets `spark.conf.set("spark.sql.session.timeZone","UTC")` in `main()`, then buckets with `to_date(to_timestamp(col))`, the **same construction the oracle uses to define truth** [A/*/pipeline.py:e.g. seed42:21; [`output_oracles.py:101,195-199`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/output_oracles.py#L101)], so A's day-set equals the truth day-set and D7 never fires (0/12 seeds). SDP (B) authors inside `@dp.materialized_view`, where `spark.conf.set(...)` is **the D5 immutable-config gate** (`CANNOT_MODIFY_CONFIG` / SQLSTATE 46110) [[`oracles.py:47-49`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/oracles.py#L47-L49)]; B's own transcript shows the agent writing the `session.timeZone=UTC` fix and then abandoning it ("UTC calendar day *without mutating* spark.sql.session.timeZone") `[B/seed1337 transcript]`. With no session-tz lever, B hand-rolls tz-*dependent*, payment/rate-**asymmetric** day math (`to_utc_timestamp(ts, current_timezone())`, `epoch//86400`, `date_from_unix_date`) that shifts naive-UTC instants by +7h and buckets the payment and rate sides under different assumptions → invents calendar days → D7 ships `[B/seed{1337,8675,11235}/…/pipeline.py:65/71/47]`.

**D8 (row-drop, `p1_medallion`): the same wall, plus a code-completeness gap.** Both arms end the validated layer with the *same* silent-drop filter (`.where(amount.isNotNull() & ts.isNotNull())`) and neither writes a quarantine table, so the drop is a shared control, not the discriminator. B loses two ways: (i) **4/8 cells omit the epoch-millis parse branch** A carries, so 13-digit epoch strings → NULL → dropped (offline replay: 201–267 rows / $41k–$55k per run) `[A/seed11235:71-89 vs B/seed11235:28-44]`; (ii) the other 4 cells hit the same session-tz wall (can't pin UTC → `to_date` mis-buckets epoch rows) `[B/seed{9001,31415,16180,14142}]`.

**The finding, mechanistically.** SDP's immutable-config property (D5) removes the one-line `session.timeZone=UTC` fix that imperative *and the oracle's own truth* rely on, so the SDP agent must instead get a careful column idiom exactly right. The base `pyspark-sdp/SKILL.md` is **silent on timezone** (0 references) and arm B loads no safety skill [[`skills/pyspark-sdp/SKILL.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/skills/pyspark-sdp/SKILL.md)], so the agent, denied the lever and untaught the replacement, hand-rolled the broken math above. **This raises the *difficulty* of correct timezone handling; it does not make it impossible**, the distinction the A/B skill test below resolves.

**The attribution: it was the skill.** A controlled skill-swap A/B test re-ran arm B on the three D7-shipping tasks (`p8_currency_normalize`, `p14_fx_settlement`, `new_stream_stream_join` × 12 seeds = 36 cells) with the `pyspark-sdp` skill *augmented by the UTC column idiom*; the frozen skill restored immediately after, so the instrument stays clean. **D7 ships went 7 → 0**, every timezone defect eliminated, cells still completing (D7 resolves to `n/a`, not a failure) `[src: results.tzfix.jsonl · per_defect_detection['D7']=='never' · arm B · 2026-07-02]`. So the immutable-config constraint is real but does **not** force the defect: it raises the difficulty, and a paradigm-appropriate skill closes the gap to parity. The raw §1.4.1 B-worse residue is therefore **skill-induced, not paradigm-inherent** (D8, the other driver, is a paradigm-neutral wash). This is the validated form of remediation #2 below.

**Engineering remediation (for framework / skill owners).**
1. **Framework (highest leverage):** OSS SDP / `pyspark.pipelines` offers no *symmetric, declarative* way to pin `session.timeZone`. Add a `spark-pipeline.yml` `configuration: {spark.sql.session.timeZone: UTC}` block (or `@dp.materialized_view(session_time_zone=…)`) applied before any view evaluates. Imperative gets this for free; SDP has no equivalent, forcing fragile hand-rolled epoch math.
2. **Skill / idiom:** teach `pyspark-sdp` the column-level UTC idiom (config is immutable): `to_date(to_utc_timestamp(ts, src_tz))` applied *identically* on every joined side, always with an epoch-millis parse branch; never mix tz-shifted math on one side with a bare `to_date` on the other.
3. **Contract:** change the validated-layer contract from *drop* to *quarantine + reconcile* (`raw_count == validated_count + rejected_count`), turns D8 from a silent completion into a loud, gate-catchable failure for both arms.

> **Framing, resolved by the data.** The raw B-worse residue is **not** a paradigm effect. Its main driver (D7) is a *skill* gap that closes entirely with a UTC column idiom (§SM1 · `results.tzfix.jsonl` · **7→0**); D8 is a paradigm-neutral wash. F1's residue clause is re-locked to: **with a paradigm-appropriate skill the silent residue is comparable across paradigms; the base `pyspark-sdp` skill's silence on UTC handling, not the declarative paradigm, drove the raw gap.** The correct headline: *"structure alone isn't enough: it needs a skill that teaches the paradigm-matched idiom; once it has one, parity."* Structural-catch (§1.4.2) and conciseness (§1.4.3) are unaffected.

## SM2. Gate-design history & retired arms (full detail)

*Why the clean two-arm design carries no gate-rigor confound, and what the retired A2 arm showed. The powered run uses bare arm A (no gate) and arm B (SDP framework dry-run); the material below is the history behind that choice, kept for reviewers.*

**Gate-validity verdict (cited).** The imperative gate is NOT a harness no-op (the prior sham-gate concern does not describe the current instrument). It runs the agent own `pipeline.py --analyze-only` [[`live.py:747-750`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/backends/live.py#L747-L750), 826-829; [`local.py:433-486`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/backends/local.py#L433-L486)] and caught 2 genuine structural errors in the A2 rerun: `UNRESOLVED_COLUMN` (p10_scd2/seed1337), `ATTRIBUTE_NOT_SUPPORTED` (new_udf_classifier/seed2718). Provenance clean: 66/66 A2 rows stamped `git_sha 1d28563a` (instrument-v3.1).

**BUT the gates are asymmetric; this is NOT "gate held constant, only paradigm varied":**
- SDP gate = framework-owned real dry-run (`create_dataflow_graph` / `register_definitions` / `start_run(dry=True)`) [[`sdp_dryrun.py:462-484`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/sdp_dryrun.py#L462-L484)], guaranteed structural analysis.
- Imperative gate = agent-owned `--analyze-only`; the harness does NOT enforce real analysis. A harness-enforced imperative gate (`_df.schema`) existed at commit `ae56e82` but was deliberately removed at `a64d830` (agent owns the program); PR #43 (`1d28563a`) fixed A2 output-path validity but did not restore it.
- Therefore the *pilot's* 74-vs-2 (A2-gate) difference **conflated paradigm with gate-rigor**, which is precisely why the locked design drops A2 for a **bare A (no gate)**: the clean powered contrast is **79-vs-0** (§1.4.2), where A has no gate *by construction*, so there is no gate-rigor confound left to conflate.

> **Re-checked against the data.** Structural-catch (first two sentences) is **confirmed** (§1.4.2: B=79 gate intercepts vs A=0). The residue clause is **revised**: the raw powered run showed B's silent-defect rate higher (§1.4.1: OR 1.97, p=0.0033), but a controlled skill-swap test attributes that to a **skill gap, not the paradigm**: D7, the main driver, closes **7→0** once B is taught the UTC column idiom (§SM1 · `results.tzfix.jsonl`), and D8 is a paradigm-neutral wash. Re-locked residue claim: **with a paradigm-appropriate skill the silent residue is comparable across paradigms; the base API skill's silence on UTC handling, not the declarative paradigm, drove the raw gap.**

## Citation convention (reference for the supplemental)
Every empirical number in this paper is immediately followed by a source tag so it can be independently re-derived from raw data:

> `[src: <file> · <field> · <row-filter> · recompute: <command>]`

- **Primary raw data:** `study/results/h3_a2_rerun_20260628/results.h3_combined.jsonl` (198 rows; 66 each for arms A2, B, B1; committed on `origin/dev`, instrument SHA `1d28563a`).
- **Code definitions** are cited as `file:line` against `origin/dev`.
- Any number not yet carrying a source tag is a **placeholder** and is marked `[PENDING]`. No hand-typed numbers.

---

## SM3. Methods: operational definitions (cited)
Before any result, we fix what the words mean. Each construct below (what counts as a silent defect, how defects are classified, at what stage a defect is caught, and how we separate the two kinds of cost) is defined against the instrument code and cited to `file:line`, so the endpoints are set before the data is seen and cannot be reshaped afterward.


### SM3.1 Silent defect
A run has `silent_defect = True` iff it reached COMPLETED/materialized output AND >=1 in-scope **semantic** defect class still shows residual output corruption (`rows > 0`). Trigger: `silent_defect = outcome.completed and len(silent_classes) > 0`. [def: [`harness/oracles.py:222-235`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/oracles.py#L222-L235) · schema: [`harness/schema.py:96-99`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/schema.py#L96-L99)]
Per-arm rate aggregation: [[`analysis/analyze.py:274-278`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/analysis/analyze.py#L274-L278)]; paired (task,seed) contrasts: [[`analyze.py:297-304`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/analysis/analyze.py#L297-L304)].

### SM3.2 Defect taxonomy: the structural / semantic / state split (load-bearing)
[def: [`harness/oracles.py:36-62`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/oracles.py#L36-L62)]

| Class | Defects | Gate-detectable? | Consequence |
|---|---|---|---|
| **Structural** | D1 missing/unresolved column; D4 broken DAG / missing upstream; D5 immutable-config mutation | **Yes** (`dry_run_detectable: True`) | Catchable at the dry-run gate, before execution. |
| **Semantic** | D2 timestamp misparse; D6 nondeterministic dedup; D7 timezone/day-bucket; D8 silent row-drop / absent quarantine | **No** (`dry_run_detectable: False`) | Only detectable in completed output → these ARE the silent-defect classes. |
| **State** | D3 unwatermarked dedup; D9 unbounded state | n/a | Not scored offline (`oracles.py:217-220`). |

**Key consequence for interpretation:** silent defects are *semantic by construction*, and semantic defects are *un-gateable by construction*. Any paradigm effect can therefore appear only in the **structural** defects (where the gate acts), never in the silent/semantic residue. [PAPER scope note: offline-scored classes are D1, D2, D4–D8; D3/D9 excluded, [`SUPPLEMENT-Section1.md:202`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/paper/SUPPLEMENT-Section1.md#L202)]

### SM3.3 Detection stage
`detection_stage in {dry_run, runtime, never, n/a}`. Meaning: `dry_run` = caught by the structural gate before any executor ran; `runtime` = caught during execution; `never` = shipped corrupt in completed output (⇒ silent_defect); `n/a` = did not manifest. [def: [`harness/oracles.py:19-23`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/oracles.py#L19-L23), 208-245 · enum: [`harness/schema.py:26-28`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/schema.py#L26-L28)]
Note: run-level priority is `never` > `dry_run` > `runtime` > `n/a` (NOT "earliest stage caught" as the schema comment says); Methods describes the implemented priority. [[`oracles.py:237-245`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/oracles.py#L237-L245)]

### SM3.4 Exit classes
`completed` (materialized output); `analysis_error` (failed structural/dry-run analysis); `runtime_error` (failed during execution); `max_iterations` (hit cap without green); `harness_error` + `PROPOSE_*` / `HARNESS_*` (instrument faults). [def: [`harness/schema.py:30-69`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/schema.py#L30-L69)]
Instrument-fault rows (`HARNESS_FAULT_EXIT_CLASSES`) are **excluded from all H1–H4 statistics** before aggregation. [[`harness/schema.py:56-69`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/schema.py#L56-L69) · [`analyze.py:118-121`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/analysis/analyze.py#L118-L121), 190-197]

### SM3.5 Cost: two distinct notions (kept separate on purpose)
**(N1) Token spend**: LLM tokens the agent burns to reach a correct pipeline. Fields: `input_tokens`, `output_tokens`, per-iteration `per_iteration[].tokens.*`. [schema: [`harness/schema.py:142-149`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/schema.py#L142-L149)]
**(N2) Data-processing compute**: actual Spark execution over data (the cluster/EKS cost). A *correctly* gate-rejected attempt processes **zero data** (caught at analysis time, before execution); an imperative attempt that fails at runtime has already executed and burned data-processing compute. Fields: `executor_seconds`, `cpu_seconds` (measured); `executor_seconds_wallclock` (a wall-clock proxy, NOT data compute). [schema: [`harness/schema.py:101-126`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/schema.py#L101-L126) · [`analyze.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/analysis/analyze.py) local-vs-cluster selection 553-576]

---

## SM6. Experimental Design & Run Protocol
This section is the study's pre-registration and reproducibility apparatus: the locked design, the full hypothesis tree, the corpus and seeds, the phased run with explicit human approval gates, and the exact commands, recorded so results cannot be retrofitted and any collaborator can re-run the study and recover the numbers in §1.4.

*Every "run" executes THIS written protocol. A collaborator can read this and know exactly what runs, what is measured, and where a human approves. Nothing runs that is not described here.*

### SM6.1 Design (LOCKED 2026-06-29): TWO arms
- **A** = bare imperative PySpark: no gate, no skills. (Imperative as it natively is.)
- **B** = SDP: framework dry-run gate + `pyspark-sdp` API skill. **NO safety skill.**
- **`spark-safety` SCRAPPED everywhere.** It changed silent-defect rate by 0.000 (B=23/66 vs B1=23/66) and was the most confusing knob in the design. Removing it kills the biggest reviewer confound ("did SDP win, or did you just give it safety advice?").
- **`pyspark-sdp` stays on B**: it is load-bearing SDP *API knowledge* (not safety), the fair analog of imperative being native to the base model. The residual asymmetry (B gets an API doc, A gets none) is addressed in §5.
- **A2, B1, B2 retired from the headline.** They were built for the pre-registered framing where the gate was a separable knob (clean test = B-vs-A2). Under F1 the gate is intrinsic to the paradigm, which orphaned A2 (gives imperative a gate) and made B1 a "gate-off + safety-off" arm. B2 is a separate compute-only question if ever revisited.

### SM6.2 Hypotheses (full tree)
*New 2-arm framing (A vs B). Supersedes the old prereg H1–H5; not a 1:1 remap. Pilot numbers are N=3, instrument-mixed (§SM6.4); the clean A-vs-B values are the powered run (§1.4, 528 cells, complete 2026-07-02).*

**H1, SAFETY (headline thesis):** forcing SDP collapses the user's catch-burden to the irreducible silent residue: SDP catches structural failures early at the gate; imperative surfaces them late or ships them.
- **H1.1 Structural-catch:** SDP catches structural defects (D1 unresolved column, D4 broken DAG, D5 immutable-config mutation) at the dry-run gate, pre-execution; bare imperative has no gate, so they surface at runtime or ship. *Clean powered run (§1.4.2): B=79 gate intercepts (353 iteration-level error events) vs A=0. CONFIRMED.*
- **H1.2 Failure-mode shift:** SDP's failures concentrate at gate-time (before data is touched); imperative's at runtime or as silent ships. *Measured via exit_class + detection_stage distribution. Pending.*
- **H1.3 Silent-residue invariance (predicted NULL / control):** semantic defects (D2/D6/D7/D8) are un-gateable in any paradigm, so silent-defect rate is ~equal A vs B. *Pilot: A=18/66, B=23/66, comparable.*

**H2, TOKEN COST (LLM effort to reach correct):**
- **H2.1 Tokens-to-correct:** total input+output tokens to a correct pipeline. *Direction OPEN. Not computable yet (B/B1 token fields null); needs run.*
- **H2.2 Iterations-to-correct (honest counter-signal):** pilot shows SDP uses MORE agent loops (median 3 vs 1), which may push tokens up, measured, not assumed in SDP's favor. *Interpret jointly with H5: extra iterations are justified if they convert into higher completion (see H5.3, cost-per-correct-completion); a raw iteration count is not, by itself, a verdict against SDP.*

**H3, COMPUTE COST (data processing; the cluster/EKS-relevant cost):**
- **H3.1 Wasted-compute-on-failed-attempts:** SDP's gate rejects failed attempts before execution (~0 data processed); imperative failures execute and burn compute. *Direction: SDP lower. **Per-attempt compute serialization (§SM6.6(3)) is now implemented** (branch `h3-per-attempt-compute`, offline tests green; it stamps per-attempt `executor_seconds`/`cpu_seconds`/`intercepted_at_dry_run` into `per_iteration`, and adds an analyze.py H3 reader). **Confirmed on EKS** by a 48-cell sweep (§1.4.3): imperative wastes ~1000× the compute SDP does on failed attempts (A `$0.028` vs B `≈$0`), because the dry-run rejects them before execution. Methodology + raw-data spec + runbook: `repro/H3_PLAN.md`, `repro/h3_eks/`.*
- **H3.2 Total-compute-to-correct:** *Confirmed on EKS (§1.4.3): imperative spends ~34× the total cluster compute of SDP (A `$0.032` vs B `$0.0009`). The earlier local wall-clock proxy (SDP higher) was substrate-confounded and is superseded.*

**H4, CONCISENESS:**
- **H4.1 LOC:** SDP fewer lines. *Pilot: ~42% fewer (68 vs 117). SUPPORTED.*
- **H4.2 AST nodes:** SDP smaller AST. *Pilot: ~38% fewer. SUPPORTED.*
- *Defensible half of the "less surface area" instinct: smaller code surface.*

**H5, EFFICACY ("does the agent get the job done?"):** head-to-head completion rate, A vs B. Direction-neutral: we report both arms' rates and let the data say which paradigm produces a working pipeline more often; no parity is assumed.
- **H5.1 Completion rate:** fraction of cells reaching a materialized/completed output (`exit_class == completed`), A vs B. *(Captures "did it produce anything runnable.")*
- **H5.2 Correct-completion rate (the real "job done"):** fraction reaching a CORRECT completed output (`success` = `exit_class == completed` AND `silent_defect == false`; cross-check `reached_correct`), A vs B. *(Captures "did it produce something actually right.")*
- *Clean powered run (§1.4): correct-completion (`completed` AND not silent) A=182/264 (68.9%), B=172/264 (65.2%); completion alone A=96.6%, B=97.7%. The small A-edge tracks the silent-defect gap (§1.4.1), which is skill-attributable: a paradigm-appropriate UTC skill resolves the driver (D7) at the defect level (7→0; the post-swap completion rate was not separately re-measured).*
- **H5.3 Cost-adjusted efficacy (interpret H2/H3 JOINTLY with H5):** SDP's extra iterations (H2.2) and any extra compute are a true *cost* only if they do NOT buy completion. Report **cost-per-correct-completion** (tokens / iterations / compute *per successful job*), so "SDP iterates more" is weighed against "SDP finishes more." More iterations are a win if the job gets done; a penalty only if it doesn't.
- Rationale: a paradigm that is safer and cheaper but finishes the job less often is a worse tool, not a better one, and conversely, a paradigm that costs more per attempt but completes more jobs may be the better tool. Completion is a primary outcome, measured head-to-head, and cost is scored relative to it.

### SM6.2.1 Control & rejected hypotheses
- **CONTROL, silent-defect residue (= H1.3):** reported, predicted equal across arms; the irreducible semantic residue.
- **REJECTED, "less surface => fewer TOTAL defects":** CONTRADICTED. SDP surfaced MORE total detected defects (A2=27, B=48, B1=46) and far more loop error-events, because the gate exposes errors rather than hiding them. The "less surface" instinct holds only as code economy (H4), not as defect count. Reported as a negative result, not omitted.

### SM6.3 Corpus, seeds, power, model
22 frozen tasks (`TASKS.lock.json` v3.0.0-corpus22); `SEEDS.lock.json` (v1.1.0-power) locks **12** seeds: the N=3 pilot used the first three (42/1337/2718), leaving headroom for N* up to 12. N* from calibration (§SM6.7): the Phase-1 calibration targets **80% power at α = 0.05** against the pilot-observed silent-defect effect (OR ≈ 2), which yields **N\* ≈ 260**; the powered run's N = 264 clears it. Model `claude-opus-4-8` (`study.config.json:4`). Full Materials & System detail in §SM7.

### SM6.4 What we already have (retrofit) vs what must run
- **Already CLEAN at instrument-v3.1 (`1d28563a`):** A (66 rows), A2 (66), B2 (66), on `origin/data/raw-export`.
- **OLD instrument, must re-run for clean claims:** B, B1.
- **=> the headline SAFETY run is essentially RE-RUN B (no-safety variant) on the current instrument, paired with existing clean A.** A does not need regenerating.
- **The COST/compute claim** additionally needs A AND B on ONE uniform substrate (§SM6.5), a fresh A+B run on Connect.

### SM6.5 Substrate (the real feasibility constraint)
- The validated `local` backend SPLITS by paradigm: imperative -> classic local Spark, SDP -> local Spark Connect (`runner.py:1204-1239`; `local_connect.py:1-15`). So local A-vs-B compute is NOT apples-to-apples.
- **Safety/structural claim:** substrate split is tolerable (defect detection is substrate-independent), noted as a minor threat.
- **Cost/data-compute claim:** MUST run both arms on ONE substrate = the `live` Connect backend, whose ConnectExecutor handles both paradigms (`live.py:569-581, 835-849`). This is precisely the cluster/EKS motivation, now confirmed as necessary, not scope creep.
- **Update (EKS run history, 2026-06-24):** the `live`/Connect substrate is **no longer hypothetical**: it was stood up and partially exercised on a real EKS cluster (`ssa-spark-eks`): driver + executors ran in k8s pods, **Arm A materialized tables remotely**, and the **in-cluster compute-measurement path was demonstrated** (Spark-UI stage-diff; a `spark.range(80M)` probe returned stage/executor-second readings) [[`DEVIATIONS.md:184-227`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/DEVIATIONS.md#L184-L227), 345-368]. The uniform-substrate compute run is therefore a matter of **completing the live run with per-attempt compute serialized** (§SM6.6(3), **now implemented**: branch `h3-per-attempt-compute`, offline tests green; see `repro/H3_PLAN.md`), not building the capability. **Resolved 2026-07-06:** remote **Arm B SDP** completes + grades green on EKS (ref-arch **L3 closed**, §SM7, Appendix S2-A); it took harness data-path + catalog-resolution fixes (`repro/h3_eks/`), not architecture. **H3 compute was measured on EKS** (both arms, stage-diff executor-seconds) and **confirmed by a 48-cell sweep** (§1.4.3): imperative spends ~34× the total and ~1000× the wasted compute of SDP.

### SM6.6 Instrument changes before the powered runs (each a reviewed PR you see the diff of)
1. **Redefine B**: SDP + gate + `pyspark-sdp`, drop `spark-safety` (arm-manifest change). [trivial]
2. **Token logging**: ALREADY works on current instrument; old B/B1 nulls were pre-token sweeps. Re-running B fixes it. [no code change]
3. **Per-attempt compute**: serialize per-iteration `IterationCost` (`executor_seconds`/`cpu_seconds`/`usd`/`intercepted_at_dry_run`) into `per_iteration`, needed only for the compute claim. Location `runner.py::run_episode` ~228-260. [moderate]

### SM6.7 Phased run with human gates
- **Phase 0**, instrument changes as reviewed PRs (§SM6.6).
- **Phase 1**, calibration: few tasks, N=3, on the fixed instrument. Output: per-cell token + compute cost, pilot effect sizes, projected **N\*** and **dollar figure**.
- **Approval gate**, a human approves N\* and projected cost before any powered/spending run.
- **Phase 2a (SAFETY paper):** re-run B (no-safety) at N\* on the current instrument; pair with existing clean A -> the A-vs-B structural-catch headline.
- **Phase 2b (COST addendum):** A + B on the uniform `live`/Connect substrate with per-attempt compute logging.
- **Phase 3**, analysis: `report.json` -> the §1.4 cited cells (no hand-typed numbers).
- **Phase 4**, bind the analysis into the paper, with independent cross-review.

### SM6.8 Literal commands (verified against runner.py argparse, `runner.py:1321-1344`)
Calibration (local backend, few tasks, N=3):
```bash
cd study
ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" python3 harness/runner.py \
  --backend local --config config/study.config.json --arms-dir arms \
  --tasks config/TASKS.lock.json --seeds config/SEEDS.lock.json \
  --only-tasks orders_silver_gold,p1_medallion,p2_cdc \
  --only-arms A,B --max-seeds 3 \
  --out results.calibration.local.n3.jsonl \
  --work-dir .work.calibration.local.n3 --per-cell-timeout 1800
```
Uniform-substrate run for the COST claim: identical but `--backend live` (requires a reachable Spark Connect endpoint + `ANTHROPIC_API_KEY`).
Analysis: `python3 analysis/analyze.py <out.jsonl> --tasks config/TASKS.lock.json`.

### SM6.9 Cost accounting (how each number is computed)
- **Token:** tokens-to-correct(arm) = sum of `per_iteration[:iterations_to_green].tokens.{input,output}` over `reached_correct` rows; paired A-vs-B, bootstrap CI.
- **Data compute:** per-arm total and *wasted* (failed-attempt) `executor_seconds`/`cpu_seconds`; gate-caught attempts contribute ~0; dollars via the substrate's metered rate. Requires §SM6.6(3) and the uniform Connect substrate.

---

## SM7. Methods: Materials & System
*(Data · Tasks · Agents/Models · Architecture · Execution. Placed here in the working draft; moves ahead of §1.4 Results in final layout. Every claim cited to a file:line on `origin/dev`.)*

### SM7.1 Data
Inputs are **deterministic NDJSON event streams** produced per `(task, seed)` by task-specific generators under `generators/`. The frozen `TASKS.lock.json` still records the pre-reorganization `infra/` prefix (the corpus is locked byte-for-byte); the runner maps that legacy prefix to `generators/` at resolution time. It resolves each task's `input`, applies any `input_args` (e.g. `--v3`), and invokes `python <gen> --seed <seed>`, writing to `<work_dir>/_data/<gen>_seed<seed>.ndjson` (`runner.py:625-651`); multi-input tasks generate each `aux_inputs` the same way (`runner.py:654-672`). The agent receives only **location** env vars: `AGENT_INPUT_PATH` (+ `AGENT_OUTPUT_PATH`/`AGENT_DEDUP_PATH` for local imperative; `AGENT_OUTPUT_TABLE` + `AGENT_AUX_INPUT_*` for live), a paradigm-symmetric, location-only contract (`local.py:433-443`; `live.py:713-718`; `base.py:229-259`). Each generator seeds its RNG from `--seed`, so data is a pure function of (generator, args, seed); seed 42 reproduces the registered oracle stream as a regression check.

Six substrates + an FX feed, each with **deliberately injected defect traps**:

| Generator / substrate | Entity | Injected messiness → defect classes |
|---|---|---|
| `gen_messy_orders.py` / orders (`--N 5000`, `--v3` adds rows) | order events (`order_id, merchant_id, event_time, amount, category`) | dup `order_id`, late/out-of-order, null/missing merchant, amount-as-string, mixed timestamps, malformed JSON, unknown merchants; v3 adds nested arrays/structs + HTML junk → D1/D2/D6/D7/D8 (`gen_messy_orders.py:7-17,67-136`) |
| `gen_customers_cdc.py` / cdc | customer CDC (`customer_id,…,op,seq,event_time`) | shuffled arrival (must order by `seq`), tombstone deletes with null payloads → D5/D6 (`gen_customers_cdc.py:43-53`) |
| `gen_payments.py` / payments (`--N 4000`) | payments (`…currency, amount_minor, amount, settled`) | foreign-currency silent-drop/mis-total (D8), TZ-offset near day boundary → wrong UTC-date FX (D7), bad currency codes need quarantine (`gen_payments.py:15-25,79-99`) |
| `fx.py` + `gen_fx_rates_cdc.py` / FX | daily USD rates (deterministic table) | ~12% wrong-rate-then-corrected revisions at higher `seq`; shuffled → must order by `seq` (`gen_fx_rates_cdc.py:44-57`) |
| `gen_emails.py` / emails (`--N 900`) | `{email_id, subject}` | null/empty → routing; non-ASCII urgency markers → urgent; naive classifiers misclassify (`gen_emails.py:4-18`) |
| `gen_trades.py` / trades (`--N 1200`) | trades (`…notional, event_time, side`) | string notionals, `-08:00` near day boundary, bad currencies → quarantine (`gen_trades.py:8-17`) |
| `gen_clickstream.py` / clickstream | clicks over `view<cart<checkout<purchase` | late/out-of-order (sessionize by event time), truncated JSON → DLQ, 30-min inactivity sessions (`gen_clickstream.py:8-17`) |

The shared ticket tells the agent the feeds are "genuinely messy" but describes *symptoms, not causes*, and forbids changing the output contract, mutating immutable config, or non-idempotent output (`prompts/task_prompt.md:1-28`).

### SM7.2 Task corpus (22 tasks, `TASKS.lock.json` v3.0.0-corpus22, frozen 2026-06-24; complexity 7 Low / 8 Med / 7 High)
Each task carries a ticket-style `prompt`, `complexity_bin`, `defects_in_scope`, `oracles`, and optional `invariants`/`aux_inputs`. D1,D2,D4–D8 are gradable; D3/D9 narrated as future work.

| # | id | bin | substrate | defects | task |
|--:|---|---|---|---|---|
| 1 | orders_silver_gold | Med | orders | D1,D2,D3,D6,D7,D8 | orders → silver (clean/dedup/enrich) → gold daily revenue |
| 2 | p1_medallion | Med | orders | D1,D2,D4,D8 | bronze→silver→gold medallion ETL over messy orders |
| 3 | p2_cdc | High | cdc | D1,D4,D5,D6 | hand-rolled SCD-1 + SCD-2 over CDC (window functions) |
| 4 | p3_windows | Low | orders | D2,D3,D7,D9 | event-time windowed revenue (1h × category) |
| 5 | p4_fanout | Low | orders | D1,D3,D4,D8,D9 | one stream fans out to two streaming tables |
| 6 | p5_mart | Low | cdc | D1,D4 | customer-segment mart from CDC |
| 7 | p6_dedup_watermark | Low | orders | D1,D3,D6,D9 | streaming dedup WITH watermark (bounded state) |
| 8 | p7_late_data | Low | orders | D2,D3,D7,D9 | late/out-of-order with allowed-lateness windows |
| 9 | p8_currency_normalize | Med | payments | D1,D4,D7,D8 | multi-currency → USD (FX as-of UTC date) |
| 10 | p9_enrich_join | Low | orders | D1,D3,D4,D8 | stream-static enrich join (orders × merchants) |
| 11 | p10_scd2 | High | cdc | D1,D4,D5,D6 | full SCD-2 with effective_from/to + no-overlap invariant |
| 12 | p11_schema_evolution | Med | orders | D1,D2,D5,D8 | schema-evolution-tolerant ingest, backfilled defaults |
| 13 | p12_quarantine_dlq | Med | orders | D1,D2,D6,D8 | explicit dead-letter quarantine of malformed orders |
| 14 | p13_cdc_windowed | High | cdc | D1,D3,D6,D9 | windowed change-rate aggregation over CDC |
| 15 | p14_fx_settlement | High | payments | D5,D7,D8,D9 | daily FX settlement totals per currency, UTC day-close |
| 16 | new_merge_upsert | Med | orders | D1,D5,D6 | idempotent MERGE/upsert into keyed silver |
| 17 | new_stream_stream_join | Med | payments | D1,D3,D7,D8,D9 | stream-stream temporal join payments × live FX feed |
| 18 | new_scd2_as_of_join | High | payments | D1,D4,D7 | point-in-time as-of join to SCD-2 FX dimension |
| 19 | new_cdc_tombstone | Med | cdc | D1,D6 | CDC tombstones remove customers from current state |
| 20 | new_udf_classifier | Low | emails | D1 | email-subject classifier UDF (imperative + SDP) |
| 21 | HC1_fx_trade_ledger | High | trades | D1,D2,D4,D5,D7 | HC-1: multi-stage FX trade ledger (SCD2 → as-of USD → MERGE) |
| 22 | HC2_session_funnel | High | clickstream | D1,D2,D6,D8 | HC-2: streaming session funnel (sessionize → funnel + DLQ) |

### SM7.3 Seeds
`SEEDS.lock.json` (v1.1.0-power, frozen 2026-06-23) locks 12 integer seeds: `[42,1337,2718,3141,5772,8675,9001,11235,27182,31415,16180,14142]`, selecting per-run input so **every arm sees byte-identical data for a given seed**. Seed 42 is first as the oracle-regression seed; `16180`/`14142` were appended to tighten the A–B CI (`SEEDS.lock.json:1-14`). The N=3 pilot used the first three.

### SM7.4 Agent & model
Base model `claude-opus-4-8`, shared across arms (`study.config.json:4`). Controlled sampling in the manifests is `temperature 0.0`, `top_p 1.0` (`arms/A.json:5-15`, `arms/B.json:5-15`); the manifest loader forces model/prompt/max-iterations/temperature/top_p to be **identical** across arms; only paradigm, gate, skills, allowed-commands vary (`arm_manifest.py:33-58`). `AnthropicBrain` defaults `temperature=0.0`, `top_p=1.0`, `max_tokens=16000` (the high cap leaves room for Opus adaptive thinking before the fenced code block) (`live.py:229-270`). **Decoding caveat:** for `claude-opus-4-*`, `build_request()` sends `thinking={"type":"adaptive"}` + `output_config={"effort":"high"}` and deliberately omits `temperature`/`top_p`/`top_k` (the Opus family rejects explicit sampling knobs), so temperature 0.0 is controlled *provenance* but is not transmitted for this model (`live.py:279-306`). Live calls run in a killable subprocess, 300 s request timeout, 2 retries; per-turn `input_tokens`/`output_tokens` are projected from usage onto each `Proposal` (`live.py:59-70,420-499`).

### SM7.5 Prompting
Per cell: `compose_task_prompt()` joins the shared preamble + the task's ticket `prompt`, **omitting the engineering `title`** so the prompt never leaks the fix; this is the "blind" framing (`runner.py:1146-1155`). `AnthropicBrain._system_prompt()` then appends paradigm framing (SDP: `from pyspark import pipelines as dp`, `@dp.table`/`@dp.materialized_view`, no `.start()`; imperative: own the SparkSession), each linked skill verbatim as `=== LINKED SKILL: <name> ===`, a gate instruction **only if the arm carries a gate**, and the output contract (a fenced Python block + a `COMMAND:` from allowed commands) (`live.py:319-373`). Bare arm A carries no gate, so no gate instruction is appended. A = no skills; B = `pyspark-sdp` only (safety skill scrapped per §SM6.1). The user message carries task id, dataset paths, and prior-iteration failure feedback (`live.py:375-413`).

### SM7.6 System architecture
`run_cell()` = one `(task, arm, seed)` → one `ResultRow`: makes a `<task>__<arm>__seed<seed>` workspace, generates data, instantiates brain + executor, stages input, runs the episode, blind-grades the output, aggregates cost, builds the row (`runner.py:744-828`). `run_episode()` loops to `max_iterations`: `propose → materialize → [gate] → execute → record → feedback-or-stop` (`runner.py:147-277`). Materialization is paradigm-specific: SDP → `transformations/pipeline.py` + harness `spark-pipeline.yml`; imperative → agent code verbatim to `pipeline.py`, no injected SparkSession/main/gate (`runner.py:305-405`).
- **Live executor `ConnectExecutor`** (Spark Connect for both paradigms): SDP gate = `harness/sdp_dryrun.py` (graph-aware framework dry-run); SDP execute = `pipelines/cli.py run --spec`; imperative execute = agent's `python3/spark-submit pipeline.py` with neutral env (`live.py:569-583,724-845`). (The executor also supports a gated-imperative path, the agent's `pipeline.py --analyze-only`, but it is exercised only by the retired gated arms; bare arm A runs no gate. See §SM2.) Compute is measured by a **Spark-UI stage-diff** before/after each run (`live.py:585-600,852-860`).
- **Local backend** splits by paradigm: SDP → `LocalConnectExecutor` (local single-node Connect), imperative → `LocalSparkExecutor` (classic in-process `local[*]`) (`runner.py:1204-1246`). This split is the §SM6.5 cross-paradigm compute constraint.
- **Blind grading**: the oracle (`oracles.py`) scores the materialized output against ground truth without access to the agent's reasoning; "blind" = the grader sees only output, and the prompt never saw the fix's title.

### SM7.7 Execution / run-triggering
Launched via `python3 harness/runner.py` with `--backend {replay,live,local}`, `--config config/study.config.json`, `--arms-dir`, `--tasks`, `--seeds`, `--only-arms`, `--only-tasks`, `--max-seeds`, `--out <jsonl>`, `--work-dir`, `--per-cell-timeout` (`runner.py:1321-1344`). Backends: **replay** (offline deterministic, no LLM/Spark, needs a recorded trace), **live** (Anthropic + Spark Connect, needs a reachable endpoint + `ANTHROPIC_API_KEY`), **local** (real local Spark, paradigm-split). Outputs: one JSONL row per cell to `--out`, transcripts to `--work-dir`; `analysis/analyze.py <out> --tasks config/TASKS.lock.json` aggregates to `report.json`. Each row is stamped with provenance: `git_sha`, `image_digest`, `spark_version`, `base_model_id`, which is how instrument-version contamination (§SM6.4) is detectable. Literal commands in §SM6.8.
