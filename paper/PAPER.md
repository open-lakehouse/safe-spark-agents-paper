> **Safe, Governed AI Data Engineering on Spark** · *a four-part working paper*
>
> **The question.** AI coding agents now write real data pipelines. The failure that matters is not a crash, it is a job that runs green and *silently ships wrong data*. So: can you get an agent's productivity on your production pipelines **without trusting the agent**?
>
> **The answer, in four parts.** **(1) The risk, measured.** A controlled 528-run study of the same agent writing pipelines two ways, free-form imperative code versus a declarative framework (Spark Declarative Pipelines, SDP): the declarative dry-run catches **79** structural defects before any data moves, imperative catches **0**, and because a broken pipeline is rejected before any executor starts, imperative burns roughly **34× the compute** (about **1000×** on failed runs). **(2) The agent-native dev loop.** A new inner loop — propose, gate, reconcile — that closes *before any data*; the agent emits only an inert plan and never touches data or credentials, so it can run *fully untrusted*. **(3) The platform, built and demonstrated.** An open governed stack (Spark Connect + Kubernetes + a governed catalog) whose **five per-tenant isolation layers all run on a live EKS cluster**: an agent authenticated as tenant A cannot reach tenant B by any path. **(4) Running it at fleet scale. Omnigent** holds credentials so no agent ever sees one, and its core is *demonstrated, not just argued*: from one brief, a single autonomous agent routed a live cross-vendor fleet, reviewed across vendors, and drove a custody-and-repair loop that built governed medallion pipelines for **three isolated tenants** over the live §3 platform, credential-free and contextual-policy-enforced (runnable at `deploy/omnigent/sdp-capstone/`; the quantitative fleet study is a separate paper).
>
> Each section flags its own maturity in the header. §1 and §3 carry the paper's load-bearing *measured* evidence; §4's core is *demonstrated* (the mechanism runs and built this paper's fleet capstone), with its quantitative study left to a separate paper.

# SECTION 1: Imperative vs SDP
### A Safety-and-Cost Study of AI Agents Writing Spark Pipelines

## Abstract *(Section 1)*
AI coding agents are increasingly trusted to author production data pipelines, where the failure that matters is rarely a crash but a *silent defect*: a job that runs to completion, passes its checks, and ships subtly wrong data. We ask whether the authoring **paradigm** an agent is given changes how safely it writes Spark pipelines, and at what cost. In a controlled study we hold the model, task corpus, seeds, prompt, and decoding fixed and vary only the paradigm across two arms: **A**, bare imperative PySpark, and **B**, Spark Declarative Pipelines (SDP) with its intrinsic structural dry-run and an API skill. Across 528 runs (22 tasks × 12 seeds × 2 arms; N = 264 per arm, statistically powered), SDP's dry-run intercepts **79** structural defects before any data is processed, against **0** for imperative, which surfaces the same faults only at runtime. SDP's agent also writes roughly **half the code** (−49% lines, −44% AST) at about **2.3× the tokens**, with comparable task completion. On a real EKS Spark-Connect cluster we also measure data-processing compute: because the dry-run rejects a broken pipeline before any executor starts, SDP is *categorically incapable* of burning cluster compute on a structurally-invalid pipeline; imperative spends roughly **34× the total compute** (about **1000×** on failed attempts), executing pipelines that only then fail. A raw silent-defect gap that appears to favor imperative proves, under a controlled skill-swap, to be *skill-induced* rather than paradigm-inherent: its main driver, timezone/day-bucket errors, collapses **from 7 to 0** once the SDP skill teaches a UTC idiom. We conclude that declarative structure buys an early, real safety margin on structural faults and does not, by itself, make an agent less safe on semantic ones, provided it is paired with a paradigm-matched skill.

## Introduction
Coding agents built on large language models are increasingly trusted to write data-engineering code, not toy scripts, but the batch and streaming pipelines that populate warehouses and feed downstream analytics. In that setting the failure that matters is rarely a crash. A pipeline that throws an exception is a pipeline you fix. The dangerous failure is the *silent defect*: a job that runs to completion, passes whatever checks are in place, and ships data that is quietly wrong: a dropped currency, a mis-bucketed day, a non-deterministic deduplication that changes the numbers from run to run. Silent defects are dangerous precisely because nothing announces them; they surface downstream, in a dashboard or a financial report, long after the agent has moved on.

An agent's exposure to this failure depends on more than the model. It depends on the **paradigm** the agent is asked to write in. Two are dominant on Spark. In **imperative PySpark**, the agent owns a live `SparkSession` and executes transformations directly: it builds a DataFrame and runs it. In **Spark Declarative Pipelines (SDP)**, the agent instead declares the pipeline as *desired state* (a set of `@dp.materialized_view` definitions) and a framework builds the dataflow graph and validates it with a structural **dry-run before any data is processed**. The intuition we test is simple: because SDP inspects the whole graph up front, it may catch a class of defects (unresolved columns, broken dependencies) that imperative code discovers only at runtime, or never.

This section asks whether that intuition holds, and what it costs. We run a controlled experiment that holds the model, task corpus, seeds, prompt, and decoding fixed and varies **only the paradigm**, across two arms: **A**, bare imperative PySpark (no gate, no skills), and **B**, SDP with its built-in structural dry-run and a paradigm-appropriate API skill. One asymmetry between the arms is deliberate: the dry-run gate is not something we add to arm B; it *comes with* the declarative paradigm, and imperative PySpark has no equivalent. So we do not bolt an artificial gate onto arm A either. Whether SDP's built-in gate is a fair difference or the whole point is a question we return to once the design and results are on the table (§2, §1.4.2).

Our contributions are:
1. **A controlled, pre-registered study** isolating authoring paradigm, run on a frozen instrument over 528 cells (22 tasks × 12 seeds × 2 arms; N = 264 per arm, statistically powered), with every reported number tied to raw data (§1.4, §SM6).
2. **A structural-safety result:** SDP's dry-run intercepts **79** structural defects before any data is processed, against **0** for imperative, which surfaces the same faults at runtime, a safety margin the declarative paradigm provides by construction (§1.4.2).
3. **An honest silent-defect result:** a raw residue that appears to make SDP *less* safe is shown, under a controlled skill-swap, to be **skill-induced, not paradigm-inherent**: its main driver, timezone/day-bucket errors, collapses **7 → 0** once the SDP skill teaches a UTC idiom (§1.4.1).
4. **A cost characterization:** SDP writes roughly **half the code** (−49% lines, −44% AST) at about **2.3× the tokens**, with comparable task completion (§1.4.3).
5. **A mechanistic root cause** linking a measured defect to a framework gap (SDP offers no declarative way to pin the session timezone) with concrete remediations for framework and skill owners (§SM1).

The rest of this section reads straight through: a short **reader's map** (below) decodes the running codes; **Background** introduces SDP and the silent-defect landscape; then the **design**, the **results**, the **threats to validity**, and the **conclusions**. The formal operational definitions (**§SM3**), the pre-registered run protocol (**§SM6**), and the full materials and system (**§SM7**) are collected in the **Supplemental Materials** at the end, so the study reproduces end to end without interrupting the read. The conclusion hands off to the control-boundary argument of **Section 2**.

## Reader's map: terms & codes
*The paper uses a few running codes; this is the decoder. Skip it if you already know them, and refer back when a code shows up.*

**The two arms** (the paradigm is the only thing we vary):

| arm | paradigm | gate | skill |
|---|---|---|---|
| **A** | imperative PySpark | none | none |
| **B** | Spark Declarative Pipelines (SDP) | framework dry-run (built in) | `pyspark-sdp` (API knowledge) |

**Defect classes (D1–D9)**: the bugs we deliberately seed and grade for. A structural gate can catch the structural family; it *cannot* catch the semantic family (those complete and ship, the **silent defects**); state defects aren't scored offline:

| family | codes | caught where |
|---|---|---|
| **Structural** | D1 unresolved column · D4 broken DAG · D5 immutable-config mutation | dry-run gate, before any data |
| **Semantic (silent)** | D2 timestamp misparse · D6 nondeterministic dedup · D7 timezone / day-bucket · D8 silent row-drop | only visible in output (ships) |
| **State** | D3 unwatermarked dedup · D9 unbounded state | streaming-state bugs: need a live stream to appear; **out of scope** here (future work) |

**What we measure (H1–H5):** **H1** safety (does the gate catch faults early?) · **H2** token cost · **H3** data-processing compute · **H4** conciseness · **H5** efficacy (does it finish the job?).

**Other running terms:**
- **silent defect**: a run that completes and passes its checks but ships wrong data.
- **the gate / dry-run**: SDP's structural check of the whole graph *before any data is processed*.
- **skill**: a knowledge module injected into the agent's prompt. Here it is [`pyspark-sdp`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/skills/pyspark-sdp/SKILL.md), a **minimal 164-line API reference** (how to declare views, run the dry-run), *not* safety advice and not task hints.
- **instrument**: the frozen harness + blind oracle + task corpus used to run and grade, version-pinned so results reproduce.
- **cell**: one run of one `(task, arm, seed)`. The study has 528 (22 tasks × 12 seeds × 2 arms).
- **N1 / N2**, the two costs kept separate: LLM **tokens** (N1) vs **data-processing compute** on the cluster (N2).
- **F1 / F2**, two framing decisions: **F1** = treat SDP's built-in gate as intrinsic and keep the asymmetry; **F2** = inject an artificial gate into imperative (rejected).
- **L0–L6**: the demonstration-layer ladder in Section 2 (how far the control boundary is proven).

**Platform and infrastructure terms (used from §2 on; standard pieces of the Spark/cloud stack):**
- **Spark Connect**: Spark's session-less client/server front end, the client ships a query *plan* to a shared server instead of driving a live engine. It is the single governed door every request goes through.
- **driver / executor**: the Spark process that plans and schedules work (driver) and the pods that actually run it (executors).
- **tenant**: one isolated customer or team sharing the same infrastructure; the whole of §3 is about keeping tenants from reaching each other's data.
- **mTLS**: mutual TLS, both client and server present certificates, so the caller's identity is cryptographic, not claimed.
- **IRSA** (IAM Roles for Service Accounts): keyless authentication from a Kubernetes pod to AWS, no long-lived keys.
- **vend**: the catalog issues a short-lived, prefix-scoped credential on demand, rather than handing out a standing key.
- **reconcile**: a controller (not the agent) drives live infrastructure to match the declared spec.
- **Lakekeeper**: an open, governed Iceberg-REST catalog. **OpenFGA**: a relationship-based (Zanzibar-style) authorization engine it uses to decide who may touch what.

## Background
**Imperative vs. declarative authoring on Spark.** Imperative PySpark is the paradigm the base model knows natively: the program acquires a `SparkSession`, reads inputs, and applies a sequence of DataFrame transformations that execute when an action is triggered. The agent is in full control, and fully responsible. SDP inverts this. The agent writes transformation functions decorated as materialized views (`from pyspark import pipelines as dp`; `@dp.materialized_view`), and the framework, not the agent, assembles them into a dataflow graph, resolves dependencies, and runs the pipeline. The agent never calls `.start()`, and, in the governed setting of later sections, never holds a session at all (§2).

**The structural dry-run gate.** The property that matters for safety is that SDP can *analyze the graph before it runs it*. Its dry-run (`create_dataflow_graph` → `register_definitions` → `start_run(dry=True)`, §1.4.2) resolves every view against the catalog and rejects structurally invalid pipelines (a column that does not exist, a view that depends on a missing upstream table, an attempt to mutate immutable configuration) **before a single executor touches data**. Imperative PySpark has no equivalent: absent a gate the agent simply builds and runs, so the same faults become runtime exceptions after work has already begun. This paper treats that gate as intrinsic to the paradigm rather than a separable feature (F1, §1.4.2).

**Silent defects and the defect taxonomy.** Not every defect is structural. We distinguish three families (§SM3.2): **structural** defects (unresolved columns, broken DAGs, immutable-config mutations: D1/D4/D5), which the gate *can* see; **semantic** defects (timestamp misparsing, non-deterministic dedup, timezone/day-bucket errors, silent row-drops: D2/D6/D7/D8), which no structural gate can see because the pipeline is well-formed and simply computes the wrong answer; and **state** defects (D3/D9): bugs in *streaming state*, such as an unwatermarked deduplication whose memory grows without bound. State defects only manifest in a live, long-running stream, so this study's offline output oracle cannot grade them; they are seeded in some tasks but left to future work, not scored here. The **semantic** family *is* the silent-defect surface, the runs that complete and ship corruption. The consequence for interpretation is sharp: a paradigm effect can appear only where the gate acts (on structural defects), or in how well the agent is *taught* to handle the semantic ones (§1.4.1).

[[[SVG-TAXONOMY]]]

**The `pyspark-sdp` skill.** The base model is fluent in imperative PySpark but not in SDP's newer API, so arm B is given a minimal [`pyspark-sdp` skill](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/skills/pyspark-sdp/SKILL.md): a **164-line API reference** that teaches *only* the mechanics (how to declare views, wire dependencies, and write a valid spec) and, in its own words, "says nothing about what your pipeline should compute." It is the fair analog of imperative being native to the base model (arm A needs no skill), not a safety aid: an earlier `spark-safety` skill was scrapped after it moved the silent-defect rate by 0.000. Because the skill is this minimal and task-agnostic, the results are a property of the **paradigm, not of a heavy skill doing the work**; the one small idiom this skill happens not to teach (bucketing a UTC calendar day) is exactly what the residual silent-defect gap in §1.4.1 traces to.

**Substrate.** The safety, token, and conciseness results are substrate-independent and run on a local backend. The data-processing-compute question (H3) requires both paradigms on one uniform cluster and is measured separately on Spark Connect / EKS (§SM6.5, §1.4.3). All runs use a single model, `claude-opus-4-8`, with identical decoding across arms (§SM7.4).

## What we measure, and why
The study is framed as *safety and cost together*, because a paradigm that is safer but finishes the job less often, or at prohibitive expense, is not automatically the better tool. We therefore measure five families of outcome, pre-registered as a hypothesis tree (§SM6.2) and reported in §1.4:

- **H1: Safety (the headline).** Does SDP change *where* failures are caught? We measure structural-defect catching at the gate (H1.1), the failure-mode distribution (H1.2), and, as a control, the silent semantic residue no gate can catch (H1.3). Safety here is not "fewer bugs written" but "faults caught earlier, before data is touched."
- **H2: Token cost.** How many LLM tokens does each paradigm burn to reach a correct pipeline? SDP is expected to iterate more against its gate (H2.2), an honest counter-signal we measure rather than assume away.
- **H3: Data-processing compute.** How much *cluster* compute does each paradigm spend, especially on failed attempts? A gate-rejected attempt processes zero data; a runtime failure has already executed. This is the cluster/EKS-relevant cost, measured on a uniform substrate (§1.4.3, §SM6.5).
- **H4: Conciseness.** How much code does each paradigm's agent write, in lines and AST nodes? The defensible half of the "less surface area" intuition.
- **H5: Efficacy.** How often does each paradigm actually produce a *correct* completed pipeline? Direction-neutral: we report both arms and read H2/H3 *relative* to H5 as cost-per-correct-completion (H5.3): extra iterations are a win if they buy completion and a penalty only if they do not.

Two measurement choices make these outcomes trustworthy. First, we separate the two notions of "cost", LLM tokens (N1) and data-processing compute (N2), because they answer different questions and behave differently under a gate (§SM3.5). Second, we define "silent defect" and the detection stage operationally, against the instrument code, *before* looking at results (§SM3.1–3.4), so the endpoints cannot be redefined to fit the data. The full pre-registered tree, including control and rejected hypotheses, is in §SM6.2.

## Experimental setup at a glance
*What we actually ran, and where to find it. The full operational detail lives in Supplemental §SM6–§SM7; this is the intuitive version.*

| | |
|---|---|
| **Manipulation** | one variable, authoring paradigm. Arm **A** = bare imperative PySpark; Arm **B** = SDP + framework dry-run gate + `pyspark-sdp` skill |
| **Scale** | 22 frozen tasks (7 Low / 8 Med / 7 High) × 12 seeds × 2 arms = **528 runs**; N = 264 per arm (statistically powered) |
| **Model** | `claude-opus-4-8`, identical decoding across arms; **blind grading**: the grader sees only output, never the fix |
| **Data** | deterministic synthetic event streams per (task, seed), with defect traps deliberately injected |
| **Substrate: safety / tokens / code** | local backend: imperative on classic local Spark, SDP on local Spark Connect |
| **Substrate: data-compute (H3)** | a real **Amazon EKS Spark-Connect cluster**: client-mode driver pod + dynamically-allocated executor pods, Iceberg tables on S3, mTLS-fronted ingress |
| **Instrument** | frozen (`instrument-v3.2-frozen`); every reported number cites a committed results file |

**The loop, for one cell.** For each (task, arm, seed): generate the seeded data → the agent proposes a pipeline → **[gate]** SDP dry-runs the whole graph before any data is touched (imperative has no such gate) → execute → blind-grade the output against ground truth → record cost; repeat to a fixed iteration cap (`harness/runner.py`, `run_cell` / `run_episode`).

[[[SVG-RUNLOOP]]]

**Where to find it**: study repo [`open-lakehouse/safe-spark-agents-paper`](https://github.com/open-lakehouse/safe-spark-agents-paper/tree/main/study):
- **Reproduction runbook** → [`reproduce/REPRODUCE.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/reproduce/REPRODUCE.md)
- **Frozen corpus & seeds** → [`study/config/TASKS.lock.json`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/config/TASKS.lock.json), [`study/config/SEEDS.lock.json`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/config/SEEDS.lock.json)
- **Arms & config** → [`study/arms/A.json`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/arms/A.json), [`study/arms/B.json`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/arms/B.json), [`study/config/study.config.json`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/config/study.config.json)
- **Harness & analysis** → [`study/harness/`](https://github.com/open-lakehouse/safe-spark-agents-paper/tree/main/study/harness), [`study/analysis/analyze.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/analysis/analyze.py)
- **EKS compute run (H3)** → [`study/repro/h3_eks/`](https://github.com/open-lakehouse/safe-spark-agents-paper/tree/main/study/repro/h3_eks) (runbook + `H3_EKS_INTEGRATION_LOG.md`)
- **Raw results behind the numbers** → [`study/results/results.powered.AB.n12.final.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/results/results.powered.AB.n12.final.jsonl) (528 rows), [`study/results/results.tzfix.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/results/results.tzfix.jsonl) (the D7 skill-swap)

*(The links above point at the repo's `main` branch; inline citations with line numbers stay pinned to the `paper-v1` tag so they resolve against the exact revision the numbers were produced from. The committed result JSONLs are the data behind every number; the 100s of MB of raw generated data and agent transcripts ship as `study/raw/sasa_raw_data_20260628.tar.gz` rather than loose files.)*

## 1.1 Research question
When an AI agent writes Spark pipelines, does **forcing it to use Spark Declarative Pipelines (SDP) instead of imperative PySpark** produce **safer** code, and **at what cost**? The manipulation under study is **paradigm, and only paradigm**. The dry-run gate and the safety skill are **held constant**, not varied: they are part of the controlled environment, not the treatment.

## 1.2 Design: one manipulation, controlled environment
The study rests on a single, deliberate manipulation. Everything a reader might suspect of driving a paradigm difference (the model, the tasks, the seeds, the prompt, the decoding) is held fixed, so any difference in outcome is attributable to paradigm alone. The one asymmetry we *keep* is the structural gate, because it is intrinsic to declarative authoring and cannot be given to imperative code without making it something other than imperative. The design below makes both choices explicit.

Independent variable: **paradigm** (SDP vs imperative), tested as **two arms** (design LOCKED 2026-06-29, §SM6.1). Held constant: model, task corpus, seeds, prompt, temperature, iteration cap.

| Arm | Paradigm | Gate | Skill | Role |
|---|---|---|---|---|
| **A** | imperative | none (bare) | none | bare imperative, imperative as it natively is |
| **B** | SDP (declarative) | framework dry-run (intrinsic) | `pyspark-sdp` API skill | declarative treatment |

**Headline contrast = A vs B.** This is deliberately a *paradigm-package* contrast, not a single-variable manipulation: the declarative paradigm brings its structural dry-run **by construction** (framing F1, §1.4.2), imperative has no equivalent, and injecting one would contaminate it (F2, rejected). So the gate is **part of the treatment, not a held-constant covariate**; that asymmetry *is* the finding. The earlier A2 (imperative+gate+skill) and B1 (SDP, no skill) arms are retired to `arms/supplementary/`; the `spark-safety` skill was scrapped everywhere (it moved silent-defect by 0.000 in pilot and was the largest reviewer confound). [arms: [`study/arms/A.json`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/arms/A.json), [`study/arms/B.json`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/arms/B.json)]

## 1.4 Results
**The headline first, so it is not missed: SDP's structural dry-run catches 79 structural defects before any data is processed; bare imperative catches 0** (§1.4.2). That is the load-bearing result of the study. The rest of this section fills in four connected stories around it: a **silent semantic residue** (§1.4.2's sibling, §1.4.1) where a *raw* gap appears to favor imperative but proves *skill-induced*, not paradigm-inherent, once a controlled skill-swap closes it; the **root-cause attribution** of that gap (§SM1); **cost** (§1.4.3), code size, tokens, and compute; and **what each paradigm hallucinates** (§1.4.4), where the same gate catches a whole class of hallucination cheaply. Read together: declarative structure buys an early, real safety margin on structural faults, and is not, by itself, less safe on semantic ones.

*All numbers below come from one run: the **powered A-vs-B run** of 528 cells (264 (task,seed) pairs × arms A/B), on the frozen instrument, with 0 instrument-fault rows. It is statistically powered (N = 264 ≥ 260 required), and inference uses a mixed-effects logistic model with Holm correction and bootstrap CIs. The full inference spec, the exact recompute command, and provenance are in **§SM6**.*

### 1.4.1 Silent-defect rate (semantic residue): clean A-vs-B (N=264/arm)
*An honest counter-signal, explained just below, and not paradigm-inherent.* On the one endpoint a structural gate *cannot* see, the rate of silent semantic defects, arm B (SDP) comes out slightly **higher**. Do not stop at the raw number: the rest of this subsection shows the gap is a single missing skill idiom that closes to parity once the skill teaches it.

| arm | silent-defect rate | k/n | 95% CI |
|---|---|---|---|
| A (bare imperative) | 0.277 | 73/264 | [0.223, 0.330] |
| B (SDP) | **0.326** | 86/264 | [0.269, 0.383] |

Paired A−B contrast: Δ = −0.049 [−0.098, +0.000]; **OR = 1.97** (B vs A); GLMM p = 0.0033, Holm-adjusted p = 0.0033, **significant at α = 0.05**.
[src: [`results.powered.AB.n12.final.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/results.powered.AB.n12.final.jsonl) · silent_defect · per arm + paired (task,seed), Holm over GLMM contrasts · recompute: §SM6]

**The gap is skill-attributable, not paradigm-inherent.** The raw contrast shows B higher, which would reject the "un-gateable ⇒ paradigm-invariant" expectation (§SM3.2). It is not the paradigm, though. The gap sits in two semantic classes: timezone/day-bucket (D7) and silent row-drop (D8); the largest class, dedup (D6), is a wash. A controlled skill-swap pins the driver: arm B's minimal `pyspark-sdp` skill happened to be silent on *one* idiom (how to bucket a UTC calendar day) and teaching it drives **D7 from 7 to 0**, matching imperative (`results.tzfix.jsonl`). So the honest reading is not "SDP is less safe," but that a minimal skill has to teach the paradigm-matched idiom; once it does, the paradigms reach parity. The mechanism (why the one-line fix imperative uses isn't available in SDP) and the parallel D8 analysis are in **§SM1**, kept off the main line. *(Pilot N = 3: A = 18/66, B = 23/66, comparable.)*

**Silent-defect composition: which classes, and where SDP loses.** The B-worse residue is **not uniform**: it decomposes by semantic class (shipped = `detection_stage == never`):

| class | A ships | B ships | read |
|---|---|---|---|
| D2 timestamp misparse | 1 | 3 | negligible |
| D6 nondeterministic dedup | 38 | 39 | **a wash**: both paradigms fail dedup ~equally; **not** SDP-specific |
| **D7 timezone / day-bucket** | **0** | **7** | **SDP-specific**: imperative *never* ships it |
| D8 silent row-drop / bad currency | 51 | 57 | B worse by +6, task-concentrated |

The whole A−B gap is **D7 (+7) and D8 (+6)**; D6, the largest class, is tied. D7 is the sharp one: imperative ships **zero** timezone defects, SDP ships 7 (mostly `p8_currency_normalize`), and it is exactly the skill-attributable driver that closes to 0 once B is taught the idiom. [src: [`results.powered.AB.n12.final.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/results.powered.AB.n12.final.jsonl) · per_defect_detection]

[[[SVG-COMPOSITION]]]

### 1.4.2 Structural-defect catching at the gate (gate-validity audit complete)
**Clean A-vs-B (528 cells).** Where structural defects (D1/D4/D5) are caught (defect-level, across ALL iterations; anti-bypass: a gate-caught-then-fixed error still counts):

| arm | at gate (dry_run) | at runtime | shipped |
|---|---|---|---|
| A (bare imperative, no gate) | 0 | 4 | 0 |
| B (SDP, framework dry-run) | **79** | 30 | 0 |

SDP's framework dry-run intercepts 79 structural defects (353 iteration-level error events) *before any data is processed*; bare imperative has no gate and intercepts zero: the structural catches surface at runtime (or, for semantic defects, ship). Arm A is *bare* imperative with **no structural gate by construction**, so the contrast measures each paradigm as it natively is: there is no gate-rigor to conflate.
[src: [`results.powered.AB.n12.final.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/results.powered.AB.n12.final.jsonl) · per_defect_detection / dry_run_intercepts / per_iteration · per arm × class-group × stage · recompute: §SM6 (see §9 error-taxonomy block)]

[[[SVG-WHERE]]]

**Framing (F1).** The asymmetry *is* the finding: the declarative paradigm provides a structural dry-run **by construction**, and imperative PySpark has no native structural gate. Injecting a harness-enforced gate into the imperative arm is explicitly rejected (F2): it would contaminate imperative with a declarative feature it would never naturally have. Stated claim:
> "SDP catches structural defects (D1/D4/D5) at a real framework dry-run *before any data is processed*; imperative PySpark has no equivalent and surfaces those defects at runtime or ships them."

This structural-catch claim is confirmed by the powered run (B = 79 vs A = 0); the silent-residue question is treated separately in §1.4.1 (skill-attributable, not paradigm-inherent). *(The retired A2 arm's gate audit and gate-design history are in Supplemental §SM2.)*

Anticipated objection ("you gave SDP a better gate") is answered directly: the gate was not *given* to SDP; it is intrinsic to declarative pipelines and unavailable to imperative without ceasing to be imperative. **This becomes the paper headline (silent-defect rate demoted to the one-line residue note in §1.4.1).**

### 1.4.3 Cost: clean A-vs-B

**Conciseness (H4): the declarative agent writes ~half the code.** Paired over (task,seed) on the final accepted program; Δ = A − B (positive ⇒ B wrote less; `*_body` excludes mandatory `@dp`/`def`/import scaffolding; the SDP `spark-pipeline.yml` is harness boilerplate, not counted):

| metric | B (SDP) | A (imperative) | Δ (A−B) | 95% CI |
|---|---|---|---|---|
| final_program_loc | 67.9 | 134.0 | **+66.1** | [+61.9, +70.4] |
| ast_node_count | 614.9 | 1105.5 | **+490.6** | [+453, +530] |

All CIs clear of zero: B is **~49% fewer LOC** and **~44% smaller AST** than imperative. [src: [`results.powered.AB.n12.final.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/results.powered.AB.n12.final.jsonl) · final_program_loc / ast_node_count · paired (task,seed), B-vs-A · [`analyze.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/analysis/analyze.py) conciseness block]

**Token spend (N1): SDP costs more to author.** Median tokens to a correct pipeline (264/264 cells populated in both arms; the streaming/32k brain fix closed the prior B-null gap):

| arm | input | output | total | vs A |
|---|---|---|---|---|
| A (bare imperative) | 1,436 | 9,964 | 11,524 | 1.0× |
| B (SDP) | 7,295 | 18,499 | **26,480** | **≈ 2.3×** |

Values are medians reported per field, so input + output need not sum to the total. Direction: SDP **higher**: the declarative agent iterates more against the gate (H2.2), which shows up as tokens. Interpret jointly with H5: the extra iterations are a true cost only if they do not buy completion. [src: [`results.powered.AB.n12.final.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/results.powered.AB.n12.final.jsonl) · input_tokens,output_tokens · per arm]

**Data-processing compute (N2): SDP is *categorically incapable of burning compute on a structurally-invalid pipeline*.** This is the sharpest cost result, and it is structural, not a matter of degree. Imperative PySpark runs `spark-submit`, executing over the data, and *then* discovers the pipeline is wrong; SDP's dry-run rejects a structurally-invalid pipeline **before any executor starts**. A failed imperative attempt therefore costs real cluster compute; a failed SDP attempt costs ≈ 0, *by construction*. This scopes to *structural* defects: a semantically-wrong SDP pipeline passes the dry-run, executes, and burns compute like imperative (and ships), the un-gateable residue of §1.4.1. (In the powered run, **69.5%** of arm B's attempts were intercepted at the gate, before touching data.)

Measured as real Spark executor-seconds (stage-diff) over a 48-cell A-vs-B sweep on an in-process Spark engine, priced at an m5.xlarge-equivalent rate (`$0.192`/executor-hour); valid N: A = 21, B = 23 after excluding 4 instrument-fault cells. The live-EKS confirmatory run of the same sweep is deferred to Phase 2b; the executor-second mechanism it measures is substrate-independent.

| N2 (executor-seconds, measured) | A (imperative) | B (SDP) | ratio |
|---|---|---|---|
| **wasted compute on *failed* attempts** | 521 exec-s · `$0.028` | 0.5 exec-s · `≈$0` | **~1000× (finite vs ≈0)** |
| **total compute** | 596 exec-s · `$0.032` | 17 exec-s · `$0.0009` | **~34×** |
| **cost per correct pipeline** | `$0.00033` | `$0.00005` | ~7× |

Nine of arm A's cells hit the iteration cap (each one running, processing data, then failing) while SDP's failed cells cost ≈ 0. **The dollar amounts are small because the study is small** (tiny tasks, a 4-executor cluster), not because the effect is: the mechanism scales linearly with data size, cluster size, and failure rate. [src: [`repro/h3_eks/`](https://github.com/open-lakehouse/safe-spark-agents-paper/tree/paper-v1/study/repro/h3_eks)  · [`results.h3.sweep2.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/repro/h3_eks/results.h3.sweep2.jsonl) · 48 cells]

[[[SVG-WASTE]]]

> **At production scale, netted: SDP comes out roughly `$2,000`/month cheaper** at ~1,000 pipelines/week, about `$5,000`/month of compute it never burns against about `$3,000`/month of extra tokens; for small pipelines the token premium dominates and the sign flips. *(A projection from the measured mechanism, not a measured result.)* The derivation: a real pipeline over ~100 GB whose failed attempt burns ~10 minutes across a 20-executor cluster wastes ≈ **`$0.60` per failed attempt**; an imperative agent that fails ~2× before it converges wastes ≈ `$1.20` per pipeline, so ~1,000 pipelines/week is the ≈ **`$5,000`/month** of compute SDP never spends. Against that, SDP's ~15k extra tokens per pipeline (§1.4.3) cost ≈ `$0.73` at representative Opus-class rates (~`$15`/`$75` per million input/output tokens), ≈ **`$3,000`/month** at the same scale. All recomputable from the token deltas above.

[[[SVG-COST]]]

### 1.4.4 What each paradigm hallucinates: two profiles, caught in two places
Beyond *whether* an agent fails, *what* it invents differs sharply by paradigm, and the difference maps onto where §1.4.2's gate can act. A **hallucination** here means the agent inventing something that does not exist, or writing code for the wrong paradigm; the silent-defect classes of §1.4.1 are the separate correctness axis and are excluded. *(A qualitative characterization over the exploratory sweeps, not a powered magnitude; runs affected.)*

| hallucination | imperative (A) | SDP (B) |
|---|---:|---:|
| invents an I/O path (nonexistent input/output location) | **51** | 0 |
| imperative session control (`spark.conf.set`) inside a declarative pipeline | 0 | **76** |
| invented / undeclared table or view | 1 | **40** |
| eager action (`.collect()`) inside a declarative query function | 0 | **27** |
| invented column name | 3 | **21** |
| invented / unsupported API | **4** | 0 |

**Imperative invents where the data lives.** Its dominant hallucination is a nonexistent I/O path (a hard-coded `.../gen_messy_orders_seed1337.ndjson`, a `.load(input_path)` that never resolves). Nothing structural can know a path is wrong until storage is touched, so it surfaces only at runtime (**96%** of imperative error-iterations) and the agent loops re-guessing, a direct source of the wasted compute in §1.4.3.

**SDP confuses the paradigm, and the gate catches it.** Its hallucinations are imperative habits leaking into the declarative frame: `spark.conf.set(...)` inside a pipeline (76 runs; 80 of 96 occurrences rejected at the dry-run gate and then removed, so ~0 survive in final SDP programs), and eager `.collect()` / `.count()` inside a query function (27 runs); plus invented undeclared tables (40, e.g. `.read.table("orders_parsed")`) and columns (21). The *same* `spark.conf.set` line is legitimate in the imperative arm (63 final programs keep it, no error): the identical keystroke is a gate-caught hallucination in one paradigm and correct in the other. **39%** of the SDP arm's error-iterations are caught at the cheap dry-run gate, versus **3%** of imperative's.

Neither shipped arm hallucinated Databricks DLT (`import dlt`, `@dlt.table`): **0 in both**. Imperative has no framework to confuse; the SDP arm's governed `pyspark-sdp` skill keeps it on the OSS API. *(The no-skill counterfactual is a separate ablation, not measured here.)*

So §1.4.2's gate does more than catch structural defects: it catches a whole *class of hallucination* (paradigm-confusion) cheaply, before compute, while imperative's dominant hallucination is un-gateable and bites only at runtime. [src: [`study/raw/raw_20260628/all_results.jsonl`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/raw/raw_20260628/all_results.jsonl) · per_iteration error_class × arm × stage · recompute: [`study/analysis/hallucination_taxonomy.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/analysis/hallucination_taxonomy.py)]

[[[SVG-HALLUCINATION]]]

## 1.5 Threats to validity
- **Imperative-gate asymmetry.** Arm A is *bare* imperative with no structural gate, so the clean structural contrast (79-vs-0) carries no gate-rigor confound: the asymmetry is intrinsic to the paradigms, not an artifact of the harness (§1.4.2). (SDP's separate "identical residue" expectation is revised by the data; see §1.4.1.)
- **Substrate split.** Imperative runs on local Spark and SDP on Connect, which blocks a fair *executor-seconds* comparison, but H1 (safety), H2 (tokens), and H4 (conciseness) are substrate-independent and unaffected. The compute claim (H3) is measured separately on the uniform EKS Connect substrate (§1.4.3).
- **Sample size.** The powered run is complete: N = 264 (task,seed) cells ≥ 260 required, with 0 instrument-fault rows. The silent-defect endpoint proved informative rather than null: arm B is significantly worse in the raw data (§1.4.1, OR 1.97, p = 0.0033), which the skill-attribution then explains.
- **Token instrumentation.** Both arms are fully token-populated (264/264); B ≈ 2.3× A (§1.4.3).

---

## 1.8 Conclusions
The study isolates paradigm and finds a clear, defensible safety asymmetry. When an agent writes in Spark Declarative Pipelines, the framework's structural dry-run intercepts unresolved columns, broken dependency graphs, and immutable-config mutations **before any data is processed**: 79 such defects caught at the gate across the powered run, against zero for bare imperative PySpark, which meets the same faults only at runtime (§1.4.2). This is not a gate we handed to SDP; it is a property SDP has by construction and imperative cannot have without ceasing to be imperative. That is the section's headline.

The counter-signal is equally important, and we report it without softening. On the *semantic* residue that no gate can catch, the raw powered run shows SDP slightly worse (silent-defect rate 0.326 vs 0.277; OR 1.97, p = 0.0033). Rather than accept "SDP is less safe," we traced the gap. It is carried almost entirely by timezone/day-bucket errors (D7) and, secondarily, silent row-drops (D8); the largest defect class, non-deterministic dedup (D6), is a wash. A three-agent code audit found the mechanism (SDP's immutable-config property removes the one-line `session.timeZone = UTC` fix imperative uses, and the base skill was silent on the replacement idiom) and a controlled skill-swap resolved the attribution: taught the UTC column idiom, arm B's D7 defects fell **from 7 to 0** (§SM1). The residue is therefore *skill-induced, not paradigm-inherent*. The honest headline is not "structure is unsafe" but **"structure alone is not enough: it needs a skill that teaches the paradigm-matched idiom; once it has one, the paradigms reach parity."**

On cost, the picture is coherent: SDP's agent writes about half the code (−49% lines, −44% AST) at roughly 2.3× the tokens: it iterates more against its gate, while completing correct pipelines at a comparable rate (65.2% vs 68.9%, a gap that itself tracks the skill-attributable D7 residue, which the UTC-idiom swap eliminates at the defect level (D7 7→0; post-swap completion not separately re-measured)). Whether the extra tokens are worth paying is a judgment about how much an early structural safety margin and half the code are worth against a token premium; the study lets that trade be made explicitly rather than assumed.

Two limitations bound these claims. The data-processing-*compute* comparison (H3) requires both paradigms on one uniform cluster and is reported separately (§1.4.3, §SM6.5); and the study fixes a single model and grants arm B an API skill arm A does not need, an asymmetry discussed in §5. Neither affects the structural-catch, token, or conciseness results, which are substrate- and skill-robust.

Finally, the safety result motivates what follows. If the most valuable thing a declarative paradigm offers is that faults can be caught, and data never touched, *before* execution, the natural next question is architectural: can we build a system in which an agent is *never* handed a live session at all, and authorship is separated from execution by construction? That is the control boundary of **Section 2**.


---
---

> **Section 1 supplemental materials.** To keep the paper readable, Section 1's methods appendix lives in a separate companion file: **[SUPPLEMENT-Section1.md](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/paper/SUPPLEMENT-Section1.md)**. It collects the root-cause forensics (§SM1), gate-design history and retired arms (§SM2), operational definitions (§SM3), the pre-registered run protocol (§SM6), and full materials and system (§SM7), everything the main text cites inline as §SM1–§SM7, retained for reproduction and deep review.

---

# SECTION 2: The Agent-Native Development Loop

## Abstract *(Section 2)*

Section 1 showed that a declarative agent writes safer and far more concise pipelines. This section is about *why* that works: the declarative paradigm is **agent-native by design**. The agent's entire job is to *declare the tables* and stop. There is no session to open, no endpoint to point at, no credential to hold, no cluster to configure, and no run to manage, so the agent writes in isolation and reasons about nothing but the transform logic itself. That minimal surface is the design goal, and it reshapes the whole developer loop around a fallible, untrusted author: rather than seat an agent in the human's write-run-debug loop (which is actively wrong for one, §2.1), we give it a loop where it emits only inert desired-state, a gate rejects bad structure up front, and a controller, never the agent, holds the credentials and reconciles.

What that buys is a **control boundary** between *authoring* and *execution*: because the agent proposes only inert desired-state, the same agent is structurally unable to run anything, misconfigure the cluster, or reach a credential. Declarative pipelines make that boundary expressible and Spark Connect enforces it, so the loop can **close at a structural dry-run gate before any data is touched**. The scope is deliberate: a well-formed pipeline that computes the wrong number (§1's silent-defect residue) still executes and is caught downstream, so the gate closes the loop before data only for *structural* defects. This is a **demonstration, not a proposal**: we built the loop and ran real agents through it, end to end and across hosts on a live EKS Spark-Connect cluster; §2.6 states honestly where it is finished and where it is not.

## Introduction

§2 takes the single developer's seat: the agent developer experience, step by step — the loop that lets you *operate* Section 1's declarative agent safely, from the point of view of one developer shipping one pipeline. The infrastructure that makes the same loop multi-tenant and production-grade — the governed catalog, Kubernetes execution, per-tenant credential vending, and the trust model — is **§3's** concern, from the infrastructure engineer's seat.

The walk-through: **§2.1** shows why building a pipeline by running it is wrong for a fallible agent; **§2.2** shows what the agent actually writes and the minimal surface that makes it agent-native; **§2.3** walks the loop end to end (author, open a PR, gate on CI, reconcile over Spark Connect); **§2.4** accounts for what the boundary buys and its one price; **§2.5** shows that dev and prod are the same loop at two endpoints; and **§2.6** states honestly where this holds today and hands off to §3.

---

## 2.1 The problem: developing a pipeline by running it is wrong for an agent
The normal way to build a Spark pipeline is imperative: write code, run it, and find out what is wrong by running it. The program executes over real data and only then surfaces an error, so every mistake costs a run. That loop was built for a person at a keyboard who mostly writes correct code, not for an agent that sometimes hallucinates.

Step back to how a pipeline normally ships. A data engineer works in an IDE or notebook against a live `SparkSession`, iterates until the job looks right, then submits it to the platform with a submission tool, `spark-submit`, a Jobs or Livy API, or an Airflow DAG, and GitOps CI/CD promotes it to production. Every step of that loop assumes a *trusted* author: one who holds a live session, warehouse credentials, and a cluster endpoint, and who owns the run and its lifecycle. That is the developer loop we are reimagining, and an untrusted agent can safely be given none of what it assumes.

The obvious way to give that agent a platform, hand it a live `SparkSession` or warehouse credentials and let it run its own code, makes each mistake maximally expensive: a wrong or hallucinated program does not merely fail, it *executes*. It can mutate cluster config, read or overwrite arbitrary tables, run unbounded operations, and, as §1 showed, ship silently-wrong data or burn real compute on pipelines that never worked. There is no governance story for "an agent holding a `SparkSession`": no gate before it touches data, no line between what it authors and what it runs, nothing to audit or contain. This section shows how to give an agent a full production data platform without ever handing it the runtime keys, and what that buys over the normal setup.

[[[SVG-DEVLOOP]]]

## 2.2 What the agent writes, and what it structurally cannot
Everything turns on one property: the agent authors an inert description of desired state, and can touch nothing else. The two paradigms make fundamentally different artifacts:

```python
# Imperative: the agent owns and runs the session; authoring IS execution.
spark = SparkSession.builder.getOrCreate()          # a live engine, in the agent's hands
df = spark.read.json(src).where(...).groupBy(...).agg(...)
df.write.saveAsTable("gold")                         # to apply it, you must RUN it

# Declarative (SDP): the agent writes only inert desired-state; the framework runs it.
@dp.materialized_view(name="gold")
def gold():                                          # no builder, no .write, no .start()
    return SparkSession.active().read.table("silver").groupBy(...).agg(...)
```

The imperative program owns the session, the reads and writes, and the lifecycle: to *apply* it you must *run* it, so there is nothing to hand a governed platform but "run this program." The SDP version is just decorated transforms; the framework owns the graph, the session, and materialization [[`runner.py:305-405`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/runner.py#L305-L405)]. The `SparkSession.active()` inside a transform is not the agent holding an engine: it is a read-only handle to the session the *framework* has already built and owns, with no way to create one, configure it, point it at a cluster, or call `.write` / `.start()`. So an agent writing SDP **builds no session it owns, and holds no credentials, endpoint, or cluster config**: it declares what the tables should be, and stops. That is what makes the paradigm *agent-native*, and the sense is positive first: the agent's entire authoring surface is *declare the tables*. Its whole API is a handful of primitives from open-source `pyspark.pipelines` (Apache Spark 4.1, not Databricks DLT), `@dp.materialized_view` / `@dp.table` to declare datasets and `read.table` to wire upstreams, with the complete reference a 164-line [`pyspark-sdp` skill](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/skills/pyspark-sdp/SKILL.md). Everything an imperative program must get right is simply **absent** from what the agent touches: no `builder.getOrCreate`, no `.write` / `.saveAsTable`, no session or cluster config, no endpoint, no credential, no submission or lifecycle. The agent writes in isolation and reasons about nothing but the transform. Only *because* that surface exposes none of those things is the same agent also structurally unable to run, misconfigure, or reach a credential: containment is what the minimal surface buys, not what *agent-native* means.

[[[SVG-CONTROLBOUNDARY]]]

## 2.3 The dev loop, step by step
Because the agent's output is inert, the loop is not an ordinary dev loop with an agent dropped into the author's seat; it is that loop *reimagined* for an untrusted author. The steps below land near a GitOps shape, but for a reason particular to agents: an author that can only emit reviewable desired-state, gated before it is applied, is exactly what a fallible agent on production data calls for. We built and ran it end to end `[study/gitops_demo/]`.

**Author.** The agent writes the pipeline as decorated transforms, and nothing more:

```python
# transformations/pipeline.py  --  what the agent produces
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

@dp.materialized_view(name="orders_silver")
def orders_silver():
    return SparkSession.active().read.table("orders_raw").where(F.col("amount") > 0)

@dp.materialized_view(name="orders_gold")
def orders_gold():
    return (SparkSession.active().read.table("orders_silver")
            .groupBy("day").agg(F.sum("amount").alias("revenue")))
```

**Open a PR.** The agent's *only* action on the outside world is to open a pull request. It adds one directory, a four-field spec plus its transforms, and nothing else:

```
pipeline-definitions/orders/
  spark-pipeline.yml            # the SDP spec: what to build
  transformations/pipeline.py   # the @dp transforms above
```

```yaml
# pipeline-definitions/orders/spark-pipeline.yml  --  the whole spec
name: gitops_demo__orders
storage: file:///…/orders/storage    # metadata + checkpoint root
catalog: spark_catalog
database: gitops_demo
libraries:
  - glob:
      include: transformations/**     # the @dp code, discovered by glob
```

That spec is the GitOps source of truth the gate and the controller both read; it is four keys and a glob, and it names no endpoint, credential, or cluster. To open the PR the agent runs only allowlisted `git` and `gh`, `git checkout -b agent/gitops/orders-<ts>` → `git commit` → `git push` → `gh pr create --base main` [[`agent_pr_author.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/gitops_demo/agent_pr_author.py)]. It has no cluster access, no credentials, and no way to run anything; a unit test enforces that on the PR-authoring harness itself, which imports no `pyspark`, never lets `SparkSession` enter `sys.modules`, and refuses to run when a live `SPARK_REMOTE` is set [[`test_agent_pr_author_no_spark.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/gitops_demo/tests/test_agent_pr_author_no_spark.py)]. The pull request is the entire interface between the untrusted agent and the platform.

**Gate on CI.** This is not illustrative; it is the workflow in this repo [[`.github/workflows/gitops-sdp-dry-run.yml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/.github/workflows/gitops-sdp-dry-run.yml)]. Whenever a PR touches a pipeline spec, GitHub Actions runs the real structural dry-run on every changed spec, and a dry-run error fails the job so the PR cannot merge:

```yaml
# .github/workflows/gitops-sdp-dry-run.yml  --  the actual PR gate
on:
  pull_request:
    paths:
      - "study/gitops_demo/pipeline-definitions/**"   # fires on the agent's spec
      # ... (also changed_pipelines.py and this workflow file)
# ...
      - name: Run the dry-run gate on each changed spec
        run: |
          for spec in "${SPECS[@]}"; do
            if python3 "${SPARK_HOME}/pipelines/cli.py" dry-run --spec "${spec}"; then
              echo "GATE PASS: ${spec}"
            else
              echo "GATE FAIL: ${spec}"; rc=1     # a structural error fails the PR
            fi
          done
          exit "${rc}"
```

`spark-pipelines dry-run` is just the Spark bin wrapper for that `pyspark/pipelines/cli.py` call [[`pyspark/pipelines/cli.py:221-263`](https://github.com/apache/spark/blob/v4.1.2/python/pyspark/pipelines/cli.py#L221-L263)]. The dry-run builds the whole dataflow graph and validates it without processing any data, via `create_dataflow_graph` then `register_definitions` then `start_run(dry=True)` (the same three calls §1's study harness makes, [[`sdp_dryrun.py:513-520`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/sdp_dryrun.py#L513-L520)]). A valid spec reports `Run is COMPLETED`; a spec that reads a table which will not exist fails right here with `[TABLE_OR_VIEW_NOT_FOUND] ... SQLSTATE 42P01`, and the PR cannot merge. In §1's study this gate caught **79 structural defects before any data was processed**, against **0** for the imperative arm, which has no such gate (§1.4.2). It checks structure, not semantics: a pipeline that is well-formed but computes the wrong result (§1's D7/D8 residue) passes here and is caught only after it runs.

**Reconcile.** Only on merge does a controller, never the agent, materialize the pipeline. The controller in this repo [[`study/gitops_demo/reconcile.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/gitops_demo/reconcile.py)] refuses to run without a Connect endpoint and shells the same spec through the stock SDP CLI:

```python
# study/gitops_demo/reconcile.py  --  the controller (never the agent)
def reconcile_spec(spec_path, *, home=None, dry=False):
    if not os.environ.get("SPARK_REMOTE"):
        raise RuntimeError("SPARK_REMOTE is not set; reconcile needs a reachable "
                           "Spark Connect endpoint (cli.py run is ONLY_SUPPORTED_WITH_SPARK_CONNECT).")
    command = "dry-run" if dry else "run"
    argv = ["python3", cli_path(home), command, "--spec", spec_path]
    return subprocess.run(argv, env=dict(os.environ)).returncode
```

This is where **Spark Connect** earns its place: it is the governed replacement for the `spark-submit` / Jobs-API step of the traditional loop (§2.1), a plan-only front door to the platform. Connect splits Spark into a thin **client** that only builds a query plan and ships it over a gRPC URL, and a **server**, the driver and its executor pods, that runs that plan inside the cluster. The controller is only the client; it holds no engine. The driver and executors live server-side on the cluster and are the only things that touch data. So a plan authored by an agent that never held a session, submitted by a controller that holds no engine, runs on a cluster the controller never logs into. The credentials belong to the controller, never the agent; which prefix each credential can read is §3.3's per-tenant vending story. The boundary isolates here because a Connect client can submit a plan but cannot reach into the engine that runs it.

**Feedback.** Structural failures from the gate become the next iteration's input, and for that class of defect the loop closes at the gate, not at runtime. (Semantic defects, right structure and wrong result, still execute and surface downstream, as in any pipeline; that is §1's residue, not something this gate claims to catch.) Imperative closes its loop only *after* execution, data read and compute spent; this one closes the structural class before.

[[[SVG-GITOPS-LOOP]]]

## 2.4 What this buys
Compared with the obvious approach, an agent holding a `SparkSession`, this turns each liability into a property, and the first property is for the agent itself. Because its whole surface is *declare the tables* (§2.2), there is no session lifecycle, endpoint, or credential to get right, so a fallible author spends its effort on transform logic instead of plumbing, part of why it writes about **half** the code of imperative (§1.4.3) and still completes the tasks (§1.4, H5). The rest are containment properties. The agent is **untrusted**: no session, no credentials, nothing to leak. Structural mistakes are **caught before any data is touched** (79 at the gate, §1.4.2) instead of discovered at runtime. A structurally-broken pipeline **cannot burn cluster compute**, because it is rejected before it runs (§1.4.3). And its output is an inert, reviewable artifact, so the loop is auditable and, because nothing the agent writes is ever run *by* the agent, governable and multi-tenant (§3). The one price is that the agent iterates more against the gate, spending roughly **2.3x** the tokens to converge (§1.4.3): you pay in cheap model calls to save data-compute, wrong data, and blast radius. For an untrusted author on production data, that is the trade you want.

## 2.5 Develop locally, promote by changing one endpoint
Because that client is only a plan-builder pointed at a URL, the same agent, spec, and dry-run gate run against a **local Connect server** while developing, where iteration is fast and free, and **promote to the remote cluster** by changing one endpoint (`SPARK_REMOTE`), with no code change. A laptop's local Connect server and the EKS cluster are the same server role at two addresses; dev and prod are the same loop at two endpoints. And the swap happens entirely *outside* the agent's surface: the agent authors one inert spec that is byte-identical in dev and prod and never sees, sets, or even names `SPARK_REMOTE`, only the controller reads it. Where the pipeline runs is not the agent's concern, and cannot be.

[[[SVG-DEVPROD]]]

There is a precise sense in which SDP keeps this promise where imperative cannot, and it starts with what Connect refuses to expose. A Connect client can build and submit a plan, but the **`SparkContext`, the JVM, RDDs, and static cluster config stay server-side, reserved for whoever administers the cluster**; Connect hands none of them to a client and errors if you reach for one (`JVM_ATTRIBUTE_NOT_SUPPORTED`, `CANNOT_CONFIGURE_SPARK_CONNECT_MASTER`). That is a privilege ladder: the **cluster admin** owns the engine (`SparkContext`, JVM, executor pods); the **controller**, as a Connect client, may submit plans but never touch it; and the **agent** holds not even a session, only an inert transform (§2.2). SDP sits on the bottom rung by construction, its declarative surface offers no way to reach for a live session at all. Imperative DataFrame code runs over Connect too and hits the same wall, but agents routinely reach for `sparkContext`, `_jvm`, or an RDD, so imperative only *happens* to stay in bounds when Connect stops it, where SDP *guarantees* it. Either way, `SparkContext` never leaves the cluster admin's side of the boundary.

[[[SVG-CONNECT-LADDER]]]

## 2.6 Where this holds today, and what §3 takes on
We ran this loop for real: a live agent (`claude-opus-4-8`) over §1's full corpus, and, separately, across hosts on a real EKS Spark Connect cluster (`ssa-spark-eks`), where the controller submitted over Connect, the driver and executors ran in Kubernetes pods, and SDP pipelines materialized to the catalog on S3. Authoring (the agent's host) and execution (the cluster) were genuinely separate machines. Two pieces are honest work in progress: in those remote runs the client reached Connect through a `socat` tunnel rather than terminating mTLS natively, and the reconciler still runs co-located on the agent's host rather than split onto the platform (`live.py:675-689`). Neither changes the boundary; both are named so nothing reads as more finished than it is. The invariants the boundary must satisfy, and the layer-by-layer build state, are collected in Appendix S2-A.

§2 draws the boundary from a single developer's seat: what the agent writes, how it opens a PR, what the gate catches, and how a controller runs it. **§3 is the platform an infrastructure engineer stands up to make that boundary real and multi-tenant**: the governed catalog, Kubernetes execution, per-tenant credential vending, and the trust model that keeps one agent's blast radius off every other tenant.


---

# SECTION 3: The Open Reference Architecture

## Abstract *(Section 3)*

§3 is the same untrusted-agent boundary of §2, seen now from the infrastructure engineer's chair: the platform that lets many mutually-untrusted agents share one cluster safely. §2 gave one developer a loop where the agent's only artifact is an inert PR and it never holds a session; §3 takes that as given and stands up the platform underneath it, adding four pieces §2 did not have: a governed **catalog** (Iceberg-REST, demonstrated on Lakekeeper + OpenFGA), **Kubernetes** execution (a client-mode Connect driver spawning executor pods on a dedicated node pool), **credential vending** (the catalog mints short-lived, prefix-scoped credentials, never the agent), and per-tenant **trust**. Its center of gravity is a five-layer per-tenant isolation stack, L1 ingress routing, L2 token custody, L3 execution isolation, L4 catalog authorization, L5 storage scoping, whose one job is to make it impossible for a tenant-A agent to reach tenant-B's data by any of five paths. All five run on a live EKS cluster today; the one capability still ahead is multi-tenant scale.

## Introduction

Where §2 took a single developer's seat, §3 takes the infrastructure engineer's: not what the agent writes, but the platform that makes the §2 boundary real, governed, and multi-tenant. The organizing idea is a separation of concerns (§3.0): each layer owns exactly one thing and delegates the rest. **§3.1** is the GitOps/CI integration boundary that tests and reconciles every change; **§3.2** is Connect-on-Kubernetes, the governed ingress and the elastic execution behind it; **§3.3 is the section's core**, the five-layer per-tenant isolation stack, walked link by link with the proof that each runs; **§3.4** states where each pillar stands today; **§3.5** hands off credential *custody* at fleet scale to §4/Omnigent.

[[[SVG-SECTION3]]]

---

**At a glance.** Because §2's agent only ever emits inert desired-state, it can be dropped into a governed data platform that trusts it with nothing. §3 builds that platform on an open stack — **SDP** for declarative authoring, a **GitOps/CI** layer that tests and reconciles every change, **Spark Connect** as the single identity-pinned front door, and **Kubernetes** for elastic execution — and closes tenant isolation along five paths, all demonstrated on a live EKS cluster: an agent authenticated as tenant A is routed to *its own* Connect server, handed a credential it never holds, run on *its own* executor pods, authorized at the catalog only for itself, and prefix-scoped at storage, so it cannot reach tenant B by any path. (The adversary and the five paths are enumerated at the top of §3.3; each layer there closes exactly one.) The five-layer isolation was re-verified fresh on 2026-07-14 (`paper/notes/proof_2026-07-14_live_isolation.log`); §3.4 maps where each pillar stands, and its Frontier row states everything still ahead.

## 3.0 The separation of concerns (the boundary this section draws)
Each layer owns one thing and **delegates the rest**; this is the section's organizing principle:

| Layer | Owns | Delegates / does NOT own |
|---|---|---|
| **SDP** | declarative authoring: *what* the pipeline is; begins and ends at the spec | execution, identity, tenant authz |
| **GitOps / CI** | continuous test + integration against a real target catalog; reconciliation; scale-out orchestration | defining the pipeline (SDP's job); enforcing grants (catalog's job) |
| **Spark Connect** | the single governed, identity-pinned **ingress** (§2 boundary, at any scale) | doing the data work itself (delegated to executors) |
| **Kubernetes** | elastic execution: client-mode driver + dynamically-allocated executor pods, containerized | the governance boundary (that stays at the Connect ingress) |
| **Catalog** *(governed Iceberg-REST; demonstrated with Lakekeeper + OpenFGA, vendor-neutral)* | tenant governance: per-principal authorization, grants, credential vending + isolation | being reinvented by SDP or GitOps |

## 3.1 GitOps / CI integration boundary: *tested integration, not blind submission*
**The problem it solves.** The naive way to give an agent a data platform is to let it submit code straight to a
cluster. That has two defects at once: it hands the agent a live session (violating the §2 control boundary), and
it ships **untested, unreviewed** changes directly into production: no gate, no integration check, no audit. The
GitOps/CI boundary removes both: the agent's *only* output is a pull request, and **a controller, not the agent, 
tests and applies it.**

**The mechanism (operationalizing §2's boundary as "the agent's only artifact is a PR").**
1. **Authoring with no session (as §2 established).** The agent's only artifact is a PR carrying inert
   desired-state, a fixed-shape SDP spec (`name`, `storage`, `catalog`, `database`, a `libraries` glob), and its
   author process is unit-tested to hold no session and refuse a runtime (§2.2, §2.3). §3 takes that boundary as
   given and owns everything after the PR.
2. **PR-time gate = integration against the real catalog (CI).** A GitHub Actions workflow triggers on any PR
   touching `pipeline-definitions/**`, stands up a real Spark Connect server, ensures the target schema, resolves
   the changed specs, and runs the **SDP framework dry-run** on each
   [[`.github/workflows/gitops-sdp-dry-run.yml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/.github/workflows/gitops-sdp-dry-run.yml)]. This is the decisive difference
   from blind submission: the desired-state is **validated against actual catalog/schema state**; broken DAGs and
   missing upstream tables are caught **before merge, before any data is processed.**
3. **Merge-time reconcile (CI controller, not the agent).** On merge to `main`, a reconcile workflow runs
   `reconcile.py`, which requires `SPARK_REMOTE` and invokes `pipelines/cli.py run --spec`
   [[`.github/workflows/gitops-sdp-reconcile-local.yml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/.github/workflows/gitops-sdp-reconcile-local.yml); [`gitops_demo/reconcile.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/gitops_demo/reconcile.py)].
   Execution is owned by the governed CI controller; the agent that authored it holds no credentials and never ran it.

**The whole loop runs locally, end to end.** A valid spec dry-runs to **`Run is COMPLETED`**, and
a spec with a missing upstream fails at the gate with **`[TABLE_OR_VIEW_NOT_FOUND] … SQLSTATE 42P01`**
[[`gitops_demo/README.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/gitops_demo/README.md); [`gitops_demo/ensure_schema.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/gitops_demo/ensure_schema.py)], so the integration gate rejects a
structurally-broken pipeline before it can reconcile.

**Why this beats blind submission.** You get **review + a real integration gate against the real target catalog +
controller-owned execution + a full audit trail**, with the agent strictly outside the runtime: CI/CD discipline
applied to agent-authored data pipelines, rather than an agent firing pipelines at a cluster on trust.

**Honest scoping.**
- The full author→PR→dry-run-gate→reconcile loop runs locally, against a runner-local Spark
  Connect server, and the PR-author session-denial is unit-tested.
- **GAP:** the gate and reconcile target **runner-local Connect, not the EKS Connect endpoint**; the production-EKS
  GitOps path is **documentation-only / not wired** [[`gitops_demo/PRODUCTION_EKS.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/gitops_demo/PRODUCTION_EKS.md)]. No captured artifact of
  a real agent-opened PR (the `gh pr create` path exists in code but no public PR run is evidenced here).
- **GAP (the authoring→runtime bridge), design-only:** how a merged tenant-A artifact would be reconciled
  *as tenant A* against §3.3's mTLS gateway is not built. The intended path: the CI controller presents a
  per-tenant client certificate (SAN `spiffe://safe-spark-agents/tenant_a`) to the gateway, which routes it to
  tenant-A's Connect server exactly as an interactive agent is routed; the PR's target tenant selects the
  certificate. Wiring the controller's identity to the runtime cert principal is not yet done, and is named
  here rather than left implicit.
- **ASPIRATIONAL, not a focus:** the CI gate today is **structural** (does the graph resolve against the catalog?).
  **Data-quality / expectation tests**, the natural place to catch some of §1's silent, semantic defects (the wrong-but-runnable
  bugs no structural gate can see), are **not built**; they are the obvious extension of this layer, noted but not claimed.

## 3.2 Connect-on-Kubernetes scale: *Connect is the governed ingress; k8s is the horsepower*
*Primer for this subsection: Spark **Connect** is Spark's session-less front end, a client sends a query **plan** to a shared server that runs it, never code to execute. The **driver** is the server process that plans and coordinates a job; **executor pods** are the worker processes that actually crunch the data. mTLS is mutual TLS, where both sides present certificates.*

**Thesis.** Connect is preserved as the single mTLS/identity-pinned ingress; a client-mode driver plus dynamically-allocated executor pods, all from one container image, scale execution elastically **without bypassing the boundary**. One long-lived shared Connect driver serves many sessions: scaling happens *behind* the governed front door, never around it.

### 3.2.1 Architecture (topology)

[[[SVG-CONNECT-K8S]]]

**Governed ingress.** Only one door into the cluster is reachable from outside, and it establishes identity by cryptography, not by trusting what the caller claims. Three facts carry the boundary:

- **One external port, mutual-TLS only.** The sole externally reachable endpoint is an internal load balancer on TCP `15009`, the mutual-TLS port; the raw Spark Connect port (`15002`) is never exposed [[`connect/base/service-mtls.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/service-mtls.yaml)].
- **Identity comes from the client's certificate, not from the client's word.** An **Envoy** proxy sidecar sits in front of Connect: it requires a valid client certificate, checks it against the cluster's certificate authority, reads the caller's identity out of that certificate, discards any identity the client tried to assert, and stamps the verified identity onto the request before passing it on [[`connect/base/envoy/envoy.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/envoy/envoy.yaml)].
- **Raw Connect is unreachable except through that proxy.** The Connect server itself listens only on loopback (`127.0.0.1:15002`) [[`connect/base/deployment.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/deployment.yaml)], so the sidecar is the only way in.

The §2 boundary therefore holds no matter how execution scales behind it: every session arrives with a cryptographically-pinned principal, and there is no path around the proxy.

**Driver + executors.** The Connect server pod *is* the Spark **client-mode driver** (`spark.master=k8s://…`, `spark.submit.deployMode=client`); the long-lived Connect JVM talks to the in-cluster Kubernetes API to create **executor pods**, advertising its pod IP and fixed RPC/block-manager ports [[`connect/base/deployment.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/deployment.yaml)]. Kubernetes access control (RBAC) gives the driver's service account exactly the permission it needs and no more: create, watch, and delete pods in its own namespace, so it can manage its own executors [[`connect/base/rbac.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/rbac.yaml)]. Where those executors land is fixed by a **pod template**, they are pinned to executor-labeled nodes and spread across nodes (a `kubernetes.io/hostname` topology constraint, so losing one node cannot take a whole stage's tasks at once) [[`connect/base/deployment.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/deployment.yaml); [`connect/base/pod-templates/executor.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/pod-templates/executor.yaml)], matching the Terraform executor node group [[`terraform/eks.tf`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/terraform/eks.tf)].

The Connect driver's Spark config makes the client-mode driver plus elastic executors concrete [[`connect/base/deployment.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/deployment.yaml)]:

```properties
# deploy/eks/connect/base/deployment.yaml  --  the Connect driver's Spark args
spark.master=k8s://https://kubernetes.default.svc   # the driver schedules pods itself
spark.submit.deployMode=client                      # the Connect pod IS the driver
spark.kubernetes.executor.podTemplateFile=/opt/spark/pod-templates/executor.yaml
spark.driver.host=$(POD_IP)                          # executors dial the driver back
spark.dynamicAllocation.enabled=true                # elastic: min 0 / init 0 / max 10
spark.dynamicAllocation.maxExecutors=10
spark.executor.cores=2
spark.executor.memory=2g
```

**Elasticity.** The dynamic-allocation envelope above scales executors 0→10; the Terraform executor pool defaults `m6i.2xlarge`, min=0/max=10/desired=2 [[`terraform/variables.tf`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/terraform/variables.tf)]. This is a configured elasticity *envelope*, not a reproduced 0→10→0 autoscaling cycle: Karpenter is explicitly deferred [[`terraform/README.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/terraform/README.md)].

**One shared driver.** The Deployment is a singleton (`replicas:1`, `Recreate`) because Connect sessions are server-local and can't be spread behind one address without breaking session affinity [[`connect/base/deployment.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/deployment.yaml)]; the live cluster runs "one long-lived Spark Connect server application shared by every run" [[`DEVIATIONS.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/DEVIATIONS.md)], and the harness caches that single application id [[`harness/backends/live.py`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/backends/live.py)]. The same image (Spark 4.1.2, Iceberg 1.11.0, S3A/AWS SDK, PostgreSQL JDBC, principal-pinning interceptor jar) serves both driver and executors via role-dispatch in the entrypoint [images/spark-connect/Dockerfile:18-35,65-118; [`images/spark-connect/entrypoint.sh`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/images/spark-connect/entrypoint.sh)].

### 3.2.2 What actually ran
The topology above was stood up on a **real EKS cluster**, and one thing was measured directly: through the driver's Spark-UI REST API, a `spark.range(80_000_000).sum()` ran on the cluster's **executor pods, not in the local process**, and registered real Spark stages (executor-seconds 1.246, cpu-seconds 0.878). This is a small probe. It confirms execution genuinely left the client and ran on the cluster; it is *not* a large multi-executor parallelism benchmark, and the paper does not claim one. Reproducibility (updated 2026-07-09): the substrate is now **terraform-applied and CI/OIDC-gated**: a fresh cluster stands up via a reviewer-approved GitHub Actions apply against S3-backed state, with **no long-lived AWS keys** (GitHub OIDC assumes a scoped role). The full deploy-and-connect runbook and the end-to-end build narrative live in the repository (`paper/notes/PLATFORM_LAB_NOTEBOOK.md`).

### 3.2.3 Honest scoping
- **This runs on the live cluster.** The topology stood up on a real EKS cluster with one shared long-lived Connect driver, and execution ran on the cluster's executor pods rather than in the local process (the `spark.range` probe registered real Spark stages) [[`DEVIATIONS.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/DEVIATIONS.md)].
- **CONFIGURED-BUT-UNREPRODUCED:** elastic **0→10 executor** scale-up/down (dynamic allocation is enabled, but no captured 0→N→0 cycle); **no Cluster Autoscaler/Karpenter committed**, so node-level autoscaling is unshown [[`connect/base/deployment.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/deployment.yaml); [`terraform/README.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/terraform/README.md)].
- **§3.3 replaces this single-server baseline with per-tenant servers.** The baseline here is one singleton Connect driver (`replicas:1`) [[`connect/base/deployment.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/deployment.yaml)]. In §3.3 that single server gives way to per-tenant Connect servers behind a routing gateway, which isolate the tenants on live EKS (2026-07-10). Per-tenant isolation runs today; what is still design-only is multi-server as a *scale* mechanism: many tenants, and horizontal replicas per tenant with node autoscaling, not the per-tenant isolation topology itself. The Terraform substrate is applied and CI-gated as of 2026-07-09.

## 3.3 Tenant governance: *the multi-tenancy stack, built and demonstrated*
**Why co-tenancy, and what is at stake.** Why multiplex tenants on shared infrastructure at all, rather than give each an isolated cluster? For the same reason §3.2 shares an elastic executor pool and §3.1 shares one governed catalog: consolidation. One elastic compute pool and one governed catalog serve many teams at a fraction of the cost and operational surface of a cluster per tenant, that efficiency is the whole point of a *platform*. But consolidation is exactly what puts a hostile or hallucinating tenant-A agent one misconfiguration away from tenant-B's data, reading it or overwriting it. So the platform has to make co-tenancy safe *by construction*, not by trusting the agent. That is what the five layers below do.

**The tenancy model: how tenants share the platform (and why a hybrid).** Before the isolation *mechanism*, one design choice sets everything else: how do many tenants, potentially thousands of agents, map onto Spark Connect servers? We weighed three models.

| Model | Shape | L1 · ingress | L2 · custody | L3 · execution | L4/L5 · catalog | Scales? |
|---|---|---|---|---|---|---|
| **A** · server per tenant *(shown below)* | one server + driver + executors **per tenant** | route to the tenant's server | static per-tenant token | disjoint pods (**physical**) | per-principal | no |
| **B** · shared servers | a few servers, tenants multiplex | pin identity **per request** | **per-request vend** | shared driver + executors (**logical**) | per-principal | yes |
| **C** · hybrid *(chosen)* | shared by default **+ optional dedicated tier** | pin identity per request | per-request vend | shared, physical tier optional | per-principal | yes |

The layers L1, L2, L4, and L5 hold in every model; in the shared models L2 becomes a credential *vended per request* keyed on the pinned principal, rather than a static per-tenant injection. The one thing that fundamentally changes is **L3, execution isolation**: physical, disjoint executor pods per tenant (A) versus credential-scoped, logical isolation on shared executors (B and C).

What tips the decision is a property of §2. The risk of a shared executor depends on *what runs there*: an imperative agent runs arbitrary code and could scavenge a co-resident tenant's in-memory or shuffle data, but an **SDP agent emits only inert declarative transforms that the framework runs, never agent code** (§2), so that attack is removed by the authoring paradigm itself. Shared execution is therefore safe for declarative agents in a way it is not for imperative ones, and physical per-tenant execution drops from a *requirement* to an optional defense-in-depth tier. Isolation rests instead on identity (L1/L2) and the catalog (L4/L5), which hold on shared infrastructure.

**We chose Model C, a hybrid:** shared Connect servers by default, each session handed a credential *vended per request* (no session ever holds a standing credential, the safest and most production-grade posture), with a dedicated-pod tier available for a high-sensitivity tenant when warranted (deferred). Two properties make this production-realistic rather than a lab convenience:

- **The server pool need not be homogeneous or co-located.** A Connect server is just a governed compute endpoint; a pool can span substrates, one on AWS, one on-prem, one on Databricks, and a request routes to the *best infrastructure for that workload*. We do not assume all Connect servers are equal. Choosing where a job runs, and vending its credential per request, is the orchestration layer's job at fleet scale (§4/Omnigent).
- **At scale the pressure moves off Connect onto the catalog and storage, cleanly split.** The governed catalog is a **metastore**: it owns metadata, per-principal authorization, and credential vending, and does not sit in the write path. Concurrent-writer commits are a storage concern that whoever runs the catalog configures; our architecture writes Delta external tables to S3 over a multi-writer-safe log store. Because governance and commits live in separate layers, each scales on its own terms; catalog-side scale itself is left to future work.

**What is proven here, and what is the frontier.** The mechanism below is demonstrated on **Model A**, the per-tenant-server instance, the *strongest-isolation* end of the spectrum where every layer is physically separate. The headline the hybrid calls for, two tenants sharing *one* Connect server who still cannot reach each other (identity plus catalog plus vend holding across a shared executor), is the proof being re-earned on the EKS-native platform. We show the design and its strongest instance now; the shared-server isolation proof and catalog-scale are named as the frontier, not claimed.

**The adversary, and the five paths.** The threat is §2's: a tenant-A agent that is *fully untrusted* — it may emit code that executes on the cluster, hallucinate, or actively try to reach tenant B's data. Reaching tenant B decomposes into exactly five distinct paths (the ledger below), and closing isolation means closing all five: each path is closed by a different layer, each layer is independently defeatable, so all five must hold — a skeptical reader can check the enumeration against the layers rather than take "isolated" on faith. Crucially, the later layers cannot backstop a failure of the earlier ones: a routing or custody failure produces a *legitimately-issued* tenant-B identity, not a forgery, which the catalog and storage gates would then correctly serve. The early links keep the **identity** honest; the late links bound what an honest identity may **do**. That is why the chain is non-redundant.

[[[SVG-ADVERSARY]]]

**The walk, from here on.** The interactive figure `paper/figures/isolation-architecture.html` renders the whole path. Each of the five links below names the residual threat it uniquely closes (read against the ledger above) and the proof that it runs on live EKS (2026-07-10 unless noted). Scaling to many tenants with node autoscaling is the one open capability; the two proof-completeness seams are named after the walk.

**Trust ledger.** Each layer is trusted with exactly one thing, and the table reads as "if only this layer failed, here is what an attacker gains":

| Path closed | Layer | Trusted to enforce | Blast radius if this layer alone fails |
|---|---|---|---|
| connect | **L1** ingress routing | route by the certificate's *authenticated* principal | a tenant-A cert reaches tenant-B's Connect server |
| be handed | **L2** token custody | inject only the tenant's own token, never client-held | a session presents tenant-B's credential |
| execute | **L3** execution isolation | run on the tenant's own pods, no shared JVM | a co-resident session reads tenant-B's in-process data |
| ask catalog | **L4** catalog authz | authorize every request (including the vend) per principal | an agent asks the catalog for tenant-B's tables or credential |
| hit storage | **L5** storage scoping | downscope the vended credential to the tenant's prefix | a credential reaches tenant-B's bytes |

**Where enforcement ends in the default open stack (the baseline this section then closes, layer by layer, below).** Identity is strong at the door but not downstream. Envoy pins an unspoofable principal from the client cert SAN and the interceptor rejects a mismatched `user_id` [[`connect/base/envoy/envoy.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/envoy/envoy.yaml); [`deploy/auth/interceptor/.../PrincipalPinningInterceptor.java`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/auth/interceptor/src/main/java/com/safesparkagents/connect/auth/PrincipalPinningInterceptor.java)]: the platform *knows* who each session is. But **authorization is fleet-scoped**: a single shared Iceberg catalog and one fleet-wide cloud IAM role (via IRSA, *IAM Roles for Service Accounts*) with read/write to the entire warehouse [[`images/spark-connect/conf/spark-defaults.template.conf`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/images/spark-connect/conf/spark-defaults.template.conf); [`terraform/irsa.tf`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/terraform/irsa.tf)]. Per-principal schema isolation (a `sandbox_<principal>` naming scheme) exists only **by convention, not enforcement** [[`RUNBOOK.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/RUNBOOK.md)], and the shared open-source catalog **cannot express per-user grants** at all: it gives audit and fleet-wide grants only, no per-user grants and no row/column masking, a limitation the repo names outright [[`deploy/auth/README.md`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/auth/README.md)]. Execution is shared too: one long-lived driver, shared executors, no per-tenant pool [[`connect/base/deployment.yaml`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/deployment.yaml)].

[[[SVG-ISOLATION]]]

**The five links, outside-in.** Each is demonstrated on live EKS (2026-07-10 unless noted); each names the residual threat it uniquely closes, the attack that would still succeed with that layer removed but the other four intact.

1. **The gateway routes by who you are, not what you ask for.** This closes the first way into another tenant: connecting straight to their server. A gateway Envoy terminates the client's mTLS, derives the principal from the certificate's URI-SAN (`spiffe://safe-spark-agents/<tenant>`), and routes on that. A tenant-A certificate reaches **only** `spark-connect-tenant-a`; tenant-B only its own server; an un-granted principal is turned away (**HTTP 403**); a connection with no client certificate never clears TLS. Because the authenticated identity, not the client's choice, selects the server, no route can send a tenant-A certificate to tenant-B's server. (`paper/notes/proof_2026-07-10_ingress_routing.log`) The wire-level encoding and the defense-in-depth caveat are in the implementation notes after link 5.
2. **A tenant only ever holds its own token, and never sees it.** This closes the next way in: presenting another tenant's credential. Each tenant gets its own Connect server, configured with **only its own tenant's catalog token, injected in the server's config** and never exposed to the client. A session on tenant-A's server operates only as tenant-A; when it configures a catalog for tenant-B it holds no tenant-B credential and is refused (`NotAuthorized: Missing Authorization Header`). Because the agent never sees a token, it cannot redirect or replay one. Full custody at fleet scale, holding and rotating the vended credential, is §4/Omnigent's job; §3 shows the per-tenant server binding. (`paper/notes/proof_2026-07-10_multiserver.log`)
3. **Each tenant's work runs on its own pods, never sharing a process.** This closes a subtler way in: a co-resident session reading another tenant's in-process data. Because Connect sessions are server-local, tenant-A's work runs on executor pods owned by tenant-A's driver (2 pods, distinct IPs), disjoint from tenant-B's (a separate driver app and pod). The two tenants never share a JVM, so neither can read the other's in-memory or on-disk shuffle data or scavenge a leaked in-process credential. (`paper/notes/proof_2026-07-10_multiserver.log`)
4. **The catalog authorizes each request by principal (Lakekeeper + OpenFGA).** This closes the way in where an agent simply asks the catalog for another tenant's tables or credential. Beyond scoping the credential once issued (point 5), the catalog gates *which* principal may request *which* tenant. With OpenFGA and per-tenant OIDC identities, a tenant-A identity is denied at the catalog for tenant-B across warehouse resolution (`config`) and namespace list/create (`404`, existence hidden for a zero-relation principal), while tenant-B and the admin get `200` and an unauthenticated call is `401`. The deny is *authorization*, not nonexistence, and toggling it proves so: a `describe` grant flips `404`↔`200`. The credential-vending path itself is probed directly, not inferred: with a real table in tenant-B, tenant-B's own `loadTable` carrying the `vended-credentials` delegation returns `200` **with credentials in the response**, while a tenant-A identity gets `404` (table hidden, no credentials vended). That is the exact cross-principal vend the fleet-scoped catalog performed and this one refuses, the seam an audit of point 5 exposes, now closed. (`paper/notes/proof_2026-07-10_perprincipal_authz.log`, sections A-D)
5. **Even a leaked credential reaches only its own tenant's bytes.** This closes the last way in: an agent that somehow holds a credential whose reach includes another tenant's bytes (shown 2026-07-09). The catalog vends per-tenant, prefix-scoped STS credentials **keylessly** (IRSA assumes a downscoping role, external-id-pinned). The same vended credential replayed against the other tenant's prefix is `AccessDenied` in both directions; an ablation confirms a whole-bucket credential *would* cross, so the deny is the downscoping vend, not the base policy. CloudTrail settles that the compute uses the vend and nothing else: every warehouse object call is under the vended session, and the fleet IRSA role makes **zero** data calls. OSS HMS/Iceberg cannot express any of this; the governed Iceberg-REST catalog is what makes even storage-scoping enforceable. (`paper/notes/cloudtrail_vend_evidence.md`, `paper/notes/proof_2026-07-10_delta_and_frontier.log`)

The per-tenant vend is configured, not narrated: each tenant's storage profile names its own `key-prefix`, `sts-enabled`, and the vending role, which is external-id-pinned [[`terraform/lakekeeper-vending.tf`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/terraform/lakekeeper-vending.tf)]:

```jsonc
// warehouse-tenant_a.aws.json  --  tenant_a's storage profile (Lakekeeper)
{
  "warehouse-name": "tenant_a",
  "storage-credential": { "type": "s3", "credential-type": "aws-system-identity",
                          "external-id": "<external-id>" },
  "storage-profile": {
    "type": "s3", "bucket": "<warehouse-bucket>", "key-prefix": "tenant_a",
    "sts-enabled": true,
    "assume-role-arn": "arn:aws:iam::<ACCT>:role/…-lakekeeper-vending"
  }
}
```

At vend time Lakekeeper assumes that role, which trusts only the catalog identity and is gated by `sts:ExternalId`, and downscopes the issued STS credential to the profile's `key-prefix` via an inline session policy. So the credential handed to a tenant-A session can reach only `s3://<bucket>/tenant_a/…`, exactly the cross-prefix `AccessDenied` shown above.

**Implementation notes (ingress, link 1).** The gateway supersedes the single-tenant per-pod mTLS sidecar of §3.2.1. Over gRPC the no-route deny surfaces as an HTTP-200 reply carrying `x-routed-to=DENIED` and `grpc-message: no tenant route` (a plain HTTP request gets a real 403). Two server-side checks bar a direct in-cluster dial that skips the gateway: Spark's pre-shared-token auth (the gateway injects a `spark-connect-psk` bearer the agent never holds; it is a shared infrastructure secret, not per-tenant) and the principal-pinning interceptor (rejects a request whose `user_id` does not match the gateway-derived `x-connect-principal`). The per-tenant servers are ClusterIP-only but bind `0.0.0.0:15002`, so those two secrets, not network topology, bar a direct connection today; a `NetworkPolicy` restricting ingress to the gateway (`deploy/eks/lakekeeper/authz/netpol-tenant-servers.yaml`) is shipped as defense-in-depth but is **not yet applied** (it needs a policy-enforcing CNI and executor-to-driver allowances).

**Proven link by link, not yet as one composed request.** Each link runs on live EKS, and the per-tenant servers already compose links 2 through 5, a write through tenant-A's server draws a tenant-scoped vend from the authorization-enabled catalog and runs on tenant-A's executors. Two honest seams remain. The storage-scoping *forensic* (the CloudTrail vend-not-IRSA discriminator) was captured against the fleet-scoped catalog, while routing, custody, execution, and authorization ran against the authorization-enabled catalog; and a single request traversing all five links, a Spark job over client-cert mTLS through the gateway to authz-catalog-vended, prefix-scoped storage on the tenant's own executors, is the remaining composition step. Neither weakens a per-link claim; both are named so the reader is not sold a composed run that was not captured.

**Credential vending vs custody (the §3↔§4 line).** The catalog *vends* short-lived, scoped credentials (a catalog function) but does **not** hand them to the agent. Holding and managing the vended credential (custody + the agent interface) is the **orchestration layer's job (§4/Omnigent)**, precisely so the agent never sees a credential and §2's boundary survives at fleet scale. §3 owns the catalog as the **authority** that grants and the **vendor** that issues short-lived credentials; §4 owns **credential custody plus the agent interface**.

[[[SVG-CUSTODY]]]

**Honest scoping, per link.** The per-link evidence is points 1–5 above and the §3.4 table; two measurement caveats the per-link proofs carry, stated so they are not glossed: (i) the storage-scoping probe (a per-write delta of 12 tasks measured before/after an 8-partition shuffle under `spark.master=k8s`) proves only that execution left the driver onto a **dedicated executor pod, not the local process** — on the shared-server run both tenants happened to land on the *same* pod, so *per-tenant pod disjointness* is a separate result, established by the multi-server run (link 3, distinct pod IPs); and (ii) the cross-tenant storage denial is observed by **replaying** the tenant-scoped vended credential against the other prefix, not by an executor pod being refused in-cluster — the executor-side channel shows only that all FileIO used the vend (CloudTrail: the fleet IRSA role makes zero data calls). Full build + proof narrative: `paper/notes/PLATFORM_LAB_NOTEBOOK.md`.

### Catalog binding: Lakekeeper and Unity Catalog OSS as co-equal governed catalogs
*The governed catalog is a swappable component, not a Lakekeeper dependency. (Author disclosure: a co-author works on open-source Unity Catalog at Databricks; this evaluation names UC OSS's gaps as scrupulously as its strengths, and every UC claim is verified against v0.5.0 source or reproduced live.)*

The five isolation layers split by whether the **catalog** owns them. **L1 ingress routing, L2 token custody, and L3 execution isolation are catalog-independent**: they live at the Connect ingress, the per-tenant Connect servers (each injects *that tenant's* catalog token), and the executor pods, and carry a Unity Catalog token exactly as they carry a Lakekeeper token. **L4 per-principal authorization and L5 prefix-scoped credential vending are the catalog's job**, and both catalogs enforce them.

**Both catalogs, verified.** Each authenticates a per-tenant identity, enforces a per-principal grant on every operation *including the credential-vend path*, and vends short-lived cloud credentials downscoped to the tenant's own prefix with external-id pinning, so a cross-tenant request is refused at the catalog and a replayed credential is refused at storage.

- **Lakekeeper + OpenFGA (Iceberg-REST),** the primary binding demonstrated above: per-principal authz via OpenFGA (Zanzibar, relationship-based); cross-tenant catalog request `404`; vend refused for an un-granted tenant; STS creds downscoped and external-id-pinned. One vendor-neutral Rust binary, natively Iceberg-REST.
- **Unity Catalog OSS 0.5.0 (native plugin),** verified against v0.5.0 source and reproduced live: per-principal authz via JCasbin (RBAC over catalog/schema/table securables); the vend endpoint gated by the same grants (`@AuthorizeExpression(VEND_TABLE_CREDENTIAL)`: `READ`→`SELECT`, `READ_WRITE`→`SELECT`+`MODIFY`); the vend mints 1-hour STS `AssumeRole` creds downscoped by an inline session policy to the exact prefix, with `sts:ExternalId` pinning. On live AWS (`paper/notes/proof_2026-07-10_uc_vending.log`, **6/6 checks**): tenant A vends real downscoped `ASIA*` creds for its own prefix; the same principal is refused `PERMISSION_DENIED` for tenant B's location (both directions); and tenant A's vended credential replayed against tenant B's prefix yields S3 `AccessDenied`. And **end to end through Spark** (the UC-native connector carrying a per-tenant UC token): a query authenticated as tenant A reads only tenant A's Delta table, via UC `loadTable` authorization, then a downscoped credential vend, then an executor read of the tenant's Delta files in S3, and is refused tenant B's table at the catalog (`PERMISSION_DENIED`), both directions.

**Where Unity Catalog OSS is genuinely weaker (and it is).** The per-principal L4/L5 result holds only via UC's **native plugin path**, with authorization enabled and each tenant presenting its own non-owner token. Residual gaps, stamped to v0.5.0:

| Gap | Kind | Detail |
|---|---|---|
| Iceberg-REST path cannot express per-principal L4 | **hard** | UC's Iceberg-REST endpoint authorizes every route at metastore-`OWNER` (all-or-nothing) and is read-only / UniForm-only; per-principal grants work only on the UC-native plugin. Lakekeeper does per-principal authz natively over Iceberg-REST. |
| Authorization off by default | posture | ships `server.authorization=disable` (an allow-all authorizer that leaves vending open); isolation is opt-in, not default. |
| No row-level security / column masking / ABAC | **hard** | coarse object-level RBAC only; RLS and masks are Databricks-managed-UC features. |
| RBAC, not relationship-based | design | JCasbin ACL/RBAC with hierarchical inheritance vs OpenFGA (Zanzibar); the OSS authorizer is single and non-thread-safe under concurrent policy reload. |
| Identity mapping | setup | maps the OIDC `email` claim to a **pre-provisioned** UC user (no default JIT; group grants unverified), so many-tenant onboarding is per-user grant work. |
| Authz maturity | maturity | standing UC 0.5.0 up per-principal required patching two authz bugs (multi-frame request-body parsing; missing authorization expression on `GET /permissions`). |
| Operational weight | ops | UC server + Postgres + OIDC token-exchange + JCasbin vs Lakekeeper's single Rust binary. |

**Takeaway.** Per-principal authorization and prefix-scoped vending — the two things §3's isolation rests on — are achievable on both an Iceberg-REST-native catalog and Unity Catalog OSS 0.5.0. The full isolation proof demonstrates on Lakekeeper, and the catalog-integrated path (per-tenant token custody, per-principal authorization, downscoped vending, executor data-read isolation) reproduces **end to end through Spark on UC OSS**, with UC's weaker spots named above so the choice is informed.

## 3.4 Where each pillar stands

A quick map of where each pillar stands: what runs on the live EKS cluster today, what is configured but not yet exercised, and what is still ahead.

| Pillar | Running on live EKS | Configured but not yet run | Still ahead |
|---|---|---|---|
| GitOps/CI | PR-author session denial; dry-run+reconcile workflows; local gate smoke | EKS-target reconcile | (none) |
| Connect-on-k8s | topology; small-scale distributed exec on EKS | elastic 0→10 executors | node autoscaler |
| Tenant governance | **per-principal mTLS ingress routing** (Envoy routes by client-cert SAN: tenant-A cert reaches only tenant-A's server, un-granted principal `403`, no cert refused); **token custody + execution isolation** via two per-tenant Connect servers (server-injected token; tenant-A session refused on tenant-B; disjoint executor pods per tenant); **per-principal catalog authorization** (Lakekeeper+OpenFGA+OIDC: tenant-A identity denied at the catalog for tenant-B, both directions, every op; grants toggle); **per-tenant storage isolation** (Lakekeeper vended creds; cross-tenant `AccessDenied` both directions; cluster-side FileIO via the vend per CloudTrail, fleet IRSA 0 data calls; separate executor pod per Spark UI) | (none) | multi-tenant scale (many tenants + node autoscaling) |

**Frontier.** Three things remain: (1) **multi-tenant scale**, many tenants and node autoscaling — the one unbuilt capability and the next step; (2) a **single request composing all five links** end to end; and (3) unifying the **storage-scoping forensic** onto the authorization-enabled catalog. Two design-only seams sit under GitOps: the gate targets a runner-local Connect, not the EKS endpoint (§3.1), and wiring the controller's identity to the per-tenant runtime certificate is not built (§3.3). Everything else in this section runs on the live cluster.

**Reproduce it.** The end-to-end setup is `deploy/eks/lakekeeper/SETUP.md`: four sub-deployments, built inside-out, each standing up one or more layers and writing the proof log cited above.

[[[SVG-REPRODUCE]]]

## 3.5 What §3 unlocks: Section 4 (Omnigent)
§3 governs *one* agent: per-principal routing, token custody, execution isolation, catalog authorization, and storage scoping, all demonstrated per tenant on live EKS. That governed substrate is the precondition for an *orchestration* layer over a whole fleet of agents. **Section 4 (Omnigent)** is that layer, and its keystone, credential custody, rests directly on the isolation above.


---

# SECTION 4, Omnigent: Governed Multi-Agent Orchestration for Data Engineering
### An orchestration layer for a fleet of governed agents

**The thesis.** §3 governs *one* agent safely; **Omnigent**, an orchestration layer enacted at runtime by a single orchestrator agent that drives credential-free sub-agent workers, governs a *fleet*. A fleet is not "more agents in parallel": an orchestration layer makes many data-engineering agents cheaper, higher-quality, governed, and collectively knowledgeable — four properties N independent sessions cannot provide by construction, having no shared coordination layer. The load-bearing one is **governance**: a single **custodian** holds every per-tenant credential from §3 and enforces each tenant's contextual data policy at submit time, so the whole fleet stays credential-free and policy-bound, the §2 boundary preserved at fleet scale and resting directly on §3's isolation. The keystone and the orchestration pattern run in this section's capstone: **governance (S4.3)** and **knowledge (S4.4)** are this paper's demonstrated contribution, while the *quantitative payoff* of cost (**S4.1**) and quality (**S4.2**) is what the separate, pre-registered study measures (**S4.6**). All four trace to one spine: an agent that emits only an inert spec (§2) can be fanned out safely.

## S4.1 Cost: heterogeneous model routing
Match the model to the task: a cheap model for a trivial fix, a strong model for a refactor, a different vendor for review. Metric: **cost-per-correct-pipeline**, §1's H5.3 (cost-per-correct-completion) lifted to the fleet. The number is S4.6's.

## S4.2 Quality: cross-vendor review
A different-vendor reviewer (for example an OpenAI model reviewing a Claude-authored pipeline) catches defects that a **correlated-blind-spot** same-vendor review structurally misses. A testable catch-rate hypothesis; the number is S4.6's.

## S4.3 Governance: credential custody (the keystone)
The catalog (§3) vends short-lived scoped credentials; **Omnigent holds custody and mediates the agent-to-catalog interface, so the agent never sees a credential.** This is what preserves §2's "agent holds no creds" boundary at fleet scale: N raw sessions leak it per session, one custodian governs it once. In a run on 2026-07-10, a custodian process holds every per-tenant credential and exposes agents only a spec-in, pass-or-fail interface; on each job it mints a fresh short-lived (300s) per-tenant token, runs the work over a §3 catalog binding (UC OSS 0.5.0, executors as local Spark threads in this dedicated proof), and returns only the result. In one run a single custodian governed both tenants, minted and rotated three short-lived credentials with the agents holding none, and an agent attempting a cross-tenant read was refused (`PERMISSION_DENIED`). Governance also covers each tenant's **contextual data policy** (quarantine, PII masking, value conservation), enforced by the custodian at submit time and demonstrated on the live EKS platform in the capstone (S4.5). (`paper/notes/proof_2026-07-10_sp41_custody.log`)

[[[SVG-CUSTODIAN]]]

## S4.4 Knowledge: shared skill library
One governed, versioned skill library (`pyspark-sdp`, safety, conventions) injected fleet-wide gives correctness propagation, consistency, single-point updates, and a guaranteed knowledge floor. It is **load-bearing:** §1 measured **zero** Databricks-DLT hallucination in both arms *with* `pyspark-sdp` equipped; the without-skill failure mode (agents defaulting to DLT) is a documented ablation §1 did not run. Fleet-wide skill sharing makes fleet competence a property of the orchestrator, not luck per session. Skills are **governed artifacts** (access-controlled, mandatable per tenant). *(Static shared skills are the demonstrated mechanism; a learned or emergent fleet memory is not claimed.)*

## S4.5 The core, running on the live platform
Two things run on the live §3 platform, both the working mechanism rather than a measured number:

1. **Credential custody (S4.3):** one custodian holds and rotates every per-tenant credential while a fleet of credential-free agents submits specs and receives only pass or fail; a cross-tenant read is refused.
2. **Heterogeneous orchestration:** the model-routing, cross-vendor-review, and skill-injection pattern, run natively and autonomously by a single Omnigent agent as a governed data-engineering fleet.

**The capstone, concretely.** The capstone ran in **two stages** over the live §3 platform: first a **deterministic wrapper** (the orchestrator decomposes; a script drives routing, custody, review, and repair), then the same build **native and autonomous** (a single Omnigent agent, given one governed custodian tool, drove the whole loop itself). From one brief it built end-to-end medallions (bronze to silver to gold) for **three isolated tenants**:
- **Inject the shared skill (knowledge):** every worker authored against the one governed `pyspark-sdp` skill.
- **Decompose:** one brief into a per-tenant medallion plan.
- **Route (the cost axis):** authoring split across vendors by difficulty; the autonomous run routed a local Qwen model, Claude Opus, and an OpenAI model, re-routing live when one vendor's harness failed to start.
- **Review (the quality axis):** a different-vendor reviewer flagged silent defects the authors missed.
- **Submit through the custodian (governance):** every pipeline crossed the custodian, so the agents held no credential, and each tenant's contextual data policy (quarantine, PII masking, financial value-conservation) was enforced at submit time; in the wrapper stage the value-conservation policy correctly rejected a non-conserving first draft before repair.
- **Repair (the §2 loop at fleet scale):** on a custodian rejection the concrete error was fed back and the fleet repaired, escalating to a stronger model, until it passed.
- **Isolation held:** each medallion materialized over its own tenant, and every cross-tenant read was denied, so §3 isolation held under the fleet.

This demonstrates that the mechanism runs, not a numbers claim (S4.6). The run, its architecture diagram, and the reproducible agent are at `paper/notes/proof_2026-07-12_sp4_capstone.log`, `paper/diagrams/section4_capstone_fleet.svg`, and `deploy/omnigent/sdp-capstone/`. *(This paper's own production used the same orchestration: an orchestrator fanned `claude_code` / `codex` / `pi` sub-agents over the work, had adversarial verifiers try to refute each finding, and synthesized only the survivors, all sharing one governed skill set.)*

[[[SVG-CAPSTONE-FLEET]]]

**The contained deployment shape (architecture).** For a client, the same delivery layer deploys contained in their own cluster: the Omnigent server, the custodian, and the credential-free agent fleet as pods on the client's EKS, over the §3 platform, with one IdP governing both (the vendored Omnigent Kubernetes path, `deploy/kubernetes/`). The capstone above demonstrates the mechanism; this is how it is packaged to run.

[[[SVG-CONTAINED]]]

**Frontier.** Scale-out (many tenants with node autoscaling), credential rotation under live long-running jobs, and a learned fleet memory; the quantitative numbers are the separate study (S4.6).

## S4.6 The fleet study (the separate paper SP4.2): the numbers
The quantitative claims behind S4.1 and S4.2 are a *separate*, pre-registered experiment (its own paper, SP4.2), not part of this paper's run and not retrofitted. Its design, for completeness:
- **Cost:** heterogeneous model routing lowers **cost-per-correct-pipeline** (§1's H5.3 lifted to the fleet) against a single-strong-model fleet at equal completion. Arms: routed vs single-model; metric: dollars per correct pipeline over a task fleet.
- **Quality:** a different-vendor reviewer catches defects a same-vendor reviewer misses (correlated blind spots). Metric: cross-vendor vs same-vendor **defect catch-rate** on a seeded-defect corpus.
- **Method:** §1's instrument (frozen corpus, blind grading, provenance) extended to a fleet of orchestrated agents, powered as §1 was. It lands in its own design doc and paper; no claim in *this* paper depends on it.


---

## Appendix S2-A, Reference Architecture: The Control Boundary
*Executable spec for implementing agents. This is the SSOT target Section 2 is measured against; build work in `SECTION2_eks_connect_demo_checklist.md`.*
**TARGET / source-of-truth.** Everything else (the demo checklist, the build, the paper's claims) *derives from this*. If a component, step, or claim cannot be traced to an invariant below, it is **drift**. Cites are file:line on `origin/dev`.

> **North star.** The agent *proposes inert desired state*; a *governed control plane, on a separate host,*
> validates and executes; the agent never holds a live session, credentials, or touches data. The dev loop
> (propose → dry-run gate → reconcile/execute) *is* that boundary. Declarative makes it expressible; Spark
> Connect is the enforcement mechanism, not the motivation.

---

#### 0. The invariant (the north star written as a testable contract)
The control boundary **holds** iff ALL of these are true. Each is a checkable predicate, not a vibe:

- **I1: Authoring ⊥ Execution.** The agent emits only an inert artifact (declarative spec + transform code);
  it never runs data operations itself.
- **I2: No credentials in the agent.** The agent never holds the Connect endpoint identity (mTLS cert / PSK /
  principal), warehouse creds, or a live `SparkSession`.
- **I3: Host separation.** The process that *executes data work* runs in a different host/trust zone than the
  agent.
- **I4: Gate before data.** Structural validation (dry-run) runs and can reject **before any data is processed.**
- **I5: Governed reconciliation.** A controller the agent does not control performs the submit/execute step.
- **I6: Inertness in transit.** What crosses the boundary is a *plan/spec*, never arbitrary code handed a live
  engine.

**A demonstration that violates any I-rule is a simulation of the boundary, not the boundary.**

---

#### 1. Trust zones
- **Zone U: Untrusted authoring.** The agent + its workspace. May write files. Holds **no creds, no session.**
- **Zone C: Governed control plane.** The reconciler/controller. Holds creds, runs the SDP Connect *client*,
  presents identity to the data plane, drives propose→gate→execute→grade. **The agent cannot run code here.**
- **Zone D: Data plane (remote, EKS).** Spark Connect service (client-mode driver pod) + executor pods +
  catalog (Iceberg JDBC/HMS) + warehouse (S3). **The only place data is touched.**
- **Boundary U│C:** agent hands an inert artifact to the controller. (Enforces I1.)
- **Boundary C│D:** controller submits *plans* over an authenticated mTLS/PSK Connect channel. (Enforces I2/I3/I6.)

---

#### 2. Components (role · zone · trust)
| Component | Zone | Role | Holds creds? |
|---|---|---|---|
| Agent (LLM) | U | Proposes SDP spec + transforms as text | **No** |
| Per-cell workspace | U | Inert `spark-pipeline.yml` + `transformations/pipeline.py` [[`runner.py:369-405`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/runner.py#L369-L405)] | No |
| Reconciler / controller | C | Runs SDP CLI client; drives gate+execute; blind-grades | **Yes** |
| Dry-run gate | C→D | Structural validation before data [[`sdp_dryrun.py:462-484`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/sdp_dryrun.py#L462-L484)] | via controller |
| Connect channel | C│D | mTLS + `Bearer PSK` + `x-connect-principal` via Envoy [[`envoy.yaml:71-158`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/envoy/envoy.yaml#L71-L158)] | yes (controller-held) |
| Connect server (driver) | D | Client-mode driver in pod; builds/runs graph [[`6ff8139`](https://github.com/open-lakehouse/safe-spark-agents-paper/commit/6ff8139)] | cluster identity |
| Executor pods | D | Do the data work | cluster (IRSA) |
| Catalog | D | Iceberg JDBC / HMS [[`spark-defaults.template.conf:33-40`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/images/spark-connect/conf/spark-defaults.template.conf#L33-L40)] | cluster |
| Warehouse (S3) | D | `s3a://…` via IRSA | cluster |
| Blind oracle | C | Grades output without seeing paradigm | n/a |

---

#### 3. End-to-end flow (each arrow tagged with the invariant it enforces)
1. **propose**: agent emits spec+code (Zone U). → *I1*
2. **materialize**: inert artifact written to workspace; handed across U│C. → *I1, I6*
3. **dry-run gate**: controller submits a structural dry-run to D; **no data touched**; structural defects
   returned to agent as feedback. → *I4*
4. **reconcile/execute**: controller (Zone C, holding creds) runs the SDP CLI *client*, which ships
   DefineOutput/DefineFlow/StartRun **plans** over the authenticated Connect channel. → *I5, I6, I2*
5. **execute in D**: driver pod + executor pods run the data work against the S3 warehouse/catalog. → *I3*
6. **telemetry + result** back to C; **blind grade**. → governance closure

---

#### 4. The authenticated submission path (the C│D detail that is the crux)
- The controller runs the stock SDP CLI as a Connect **client**: it imports the agent's transform Python,
  builds the dataflow graph, and sends **protobuf plans** over gRPC: **the server needs no raw files**
  [[`pyspark/pipelines/cli.py:221-263`](https://github.com/apache/spark/blob/v4.1.2/python/pyspark/pipelines/cli.py#L221-L263); [`spark_connect_graph_element_registry.py:51-136`](https://github.com/apache/spark/blob/v4.1.2/python/pyspark/pipelines/spark_connect_graph_element_registry.py#L51-L136)]. (I6 satisfied for code.)
- **Channel:** `sc://<NLB>:15009/;use_ssl=true` + `Bearer PSK` + `x-connect-principal`, terminated by Envoy
  mTLS [[`envoy.yaml:24-158`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/connect/base/envoy/envoy.yaml#L24-L158)]. The controller, not a side-tunnel, must hold and present this identity. (I2.)
- **Data plane:** Connect server = client-mode driver pod; executors = k8s pods; warehouse = S3 via IRSA;
  catalog = Iceberg JDBC/HMS [[`6ff8139`](https://github.com/open-lakehouse/safe-spark-agents-paper/commit/6ff8139); [`spark-defaults.template.conf:33-48`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/images/spark-connect/conf/spark-defaults.template.conf#L33-L48)]. (I3.)

---

#### 5. Known boundary leaks to design against (name them, don't hide them)
- **R1: agent code executes in Zone C during plan construction.** The SDP client `exec_module`s the agent's
  transform Python to build the plan [[`cli.py:248-263`](https://github.com/apache/spark/blob/v4.1.2/python/pyspark/pipelines/cli.py#L248-L263)]. No data/creds are exposed at that instant, but it *is*
  agent-authored code running in the governed zone. Reference stance: acceptable as *plan construction* only if
  sandboxed/AST-checked; must be stated explicitly as the subtlest part of the boundary.
- **R2: controller co-located with agent.** Today the reconciler runs as a subprocess on the agent/harness host
  [[`live.py:690-704`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/backends/live.py#L690-L704)] (Zones U and C collapsed). Reference **requires them split**.
- **R3: mTLS via socat tunnel.** The prior remote runs terminated mTLS in a local `socat` tunnel, not the client
  `[study.config.live.json:2]`. That parks the creds in the tunnel host, not the controller → violates the spirit
  of I2. Reference **requires the controller itself to present identity natively**.

---

#### 6. Working backwards: dependency-ordered layers (build order = drift detector)
Each layer depends on the one below. **You cannot honestly claim a layer while a lower invariant is unproven; 
that is the definition of drift here.**

| Layer | Claim it licenses | Enforces | Depends on |
|---|---|---|---|
| **L0 Substrate** | EKS Connect server + Envoy + catalog + S3 reachable | (none) | (none) |
| **L1 Authenticated channel** | controller → Connect via **native** mTLS/PSK, no tunnel | I2 (C│D) | L0 |
| **L2 Off-host execution** | a trivial plan runs in D, driver+executors in pods | I3 | L1 |
| **L3 SDP submission green** | agent-authored SDP spec submitted from C **completes + grades green** in D | I1, I6 | L2 |
| **L4 Gate before data** | dry-run rejects structural defects pre-execution | I4 | L2 |
| **L5 Governance split** | reconciler in Zone C, agent in Zone U, agent holds **no creds** | I5, I2, (R2) | L3, L4 |
| **L6 Negative control** | imperative **cannot** traverse C│D (compatibility probe) | thesis support | L1 |

**Read top-down to find the highest layer we may honestly claim today; read bottom-up to build.**

---

#### 7. Reverse-engineering map (reference → as-built), seeded, to complete together
| Ref element | Target (invariant) | As-built status | Evidence / delta |
|---|---|---|---|
| Inert artifact | I1, I6 | **IMPLEMENTED** | `runner.py:369-405`; spec+code only |
| Plan-not-files submission | I6 | **IMPLEMENTED** (PySpark) | `cli.py:221-263` |
| L0 substrate | (none) | **APPLIED via terraform + CI (2026-07-09)**, S3-backed state (the earlier hand-built June cluster was torn down + rebuilt clean) | `deploy/eks/terraform`; `paper/notes/PLATFORM_LAB_NOTEBOOK.md` |
| L1 native mTLS/PSK | I2 | **PARTIAL**: reached via socat tunnel, native client unproven (R3) | `study.config.live.json:2` |
| L2 off-host execution | I3 | Both arms run off-host on the cluster: the driver and executors run in pods and the tables materialize | `repro/h3_eks/` |
| L3 SDP green remote | I1,I6 | Arm B SDP completes and grades green remotely (2026-07-06): the agent authors an inert spec and the CLI submits it session-less. Getting there took harness data-path and catalog fixes, not architecture | `repro/h3_eks/` |
| L4 gate before data | I4 | **IMPLEMENTED locally**; not re-proven remote | `sdp_dryrun.py:462-484` |
| L5 governance split | I5,I2,R2 | **GAP**: reconciler co-located on agent host | `live.py:675-689` |
| L6 imperative-can't-cross | thesis | **CORROBORATED, not captured**: why Part 1 went local | `DEVIATIONS.md:516-522` |

**Highest honestly-claimable layer today: ~L3** (both arms run the full loop remotely; Arm B SDP completes+grades green, 2026-07-06). L5 (governance split) is the real
remaining work; L1 needs native-mTLS de-risking; L6 needs capturing as a clean artifact.

---

#### 8. How this section may be written, by layer (anti-overclaim guide)
- Claim **only up to the highest proven layer**, and state the next gap plainly.
- "Demonstration" language is licensed for L0–L4 (both arms remote, Arm B SDP green, 2026-07-06); **L5 (governance split) must read as "remaining gap," not done.**
- The **thesis** (control boundary) is *architecturally sound and demonstrated across hosts (L3)*; the honest framing is
  "exercised across hosts on a real cluster; full SDP completion DONE (2026-07-06); governance split (L5) pending."


---

## Appendix S3-A, Reference Architecture: The Open Governed Platform
*Executable target for the multi-tenant platform §3 builds toward: the SSOT for implementing agents. Build work: `SECTION3_platform_build_checklist.md`. As with Appendix S2-A, claim only up to the highest proven layer; anything above it is a build task, not a result.*

#### G: Invariants (what the platform must satisfy)
- **G1 GitOps-only mutation.** Production state changes only via reviewed PR + CI reconcile; no direct agent submission.
- **G2 Connect-as-ingress.** All execution enters through the single governed mTLS/identity-pinned Connect endpoint; no path around it (subsumes §2's boundary).
- **G3 Identity pinning.** Each session's principal is cryptographically derived from the cert SAN, unspoofable.
- **G4 Elastic execution.** Compute scales on Kubernetes (dynamic executor pods) *behind* the ingress.
- **G5 Tenant isolation.** Per-tenant authorization (catalog grants) + per-tenant execution isolation (per-tenant multi-server Connect, demonstrated), **delegated to a governed catalog, with multi-tenant scale (many tenants + node autoscaling) still future**, not reinvented.
- **G6 Auditability.** Every change is a reviewable artifact (PR + provenance).

*Credential flow (the §3↔§4 line):* the catalog **vends** short-lived scoped credentials; the orchestration layer (§4/Omnigent) holds **custody** and mediates the agent interface; the agent stays **credential-free**: this is how G2 / §2-I2 survives at fleet scale.

#### P: Build/claim layers (bottom-up; highest proven layer = honest claim ceiling)
| Layer | Licenses the claim | Status today |
|---|---|---|
| **P0 substrate** | EKS + Connect + Envoy + catalog + S3 exist | The substrate is live: terraform-applied and CI/OIDC-gated (2026-07-09) |
| **P1 governed ingress** | mTLS + principal pinning, no bypass | **DEPLOYED + ENFORCING on EKS** (Envoy mTLS + interceptor pin principal+PSK+user_id); native pyspark client-cert path still unproven |
| **P2 elastic execution** | driver + dynamically-allocated executor pods | **DEPLOYED on EKS**; executor pods spin up; elastic 0→N→0 cycle uncaptured |
| **P3 GitOps boundary** | agent-as-PR-author + CI dry-run gate + reconcile | Runs locally today |
| **P4 integration testing** | structural dry-run against the real catalog | The structural dry-run runs against the real catalog today; data-quality tests are next |
| **P5 tenant isolation** | data isolation (credential scoping) + per-principal authz + per-tenant execution | Storage scoping holds today: Lakekeeper vends the creds, and replaying the vended cred a cross-tenant request gets `AccessDenied` both directions on EKS; the write ran on a dedicated executor pod off-driver per Spark UI, 12-task per-write delta, and per-tenant pod disjointness is the multi-server result below; CloudTrail shows cluster-side FileIO via the vend, fleet IRSA 0 data calls, and an ablation confirms the vend is load-bearing. Catalog authorization is per-principal (Lakekeeper+OpenFGA+OIDC): a tenant-A identity is denied at the catalog for tenant-B across warehouse-resolution, namespace, and the direct vend path (loadTable+delegation: tenant-B vends `200`+creds, tenant-A `404`), and grants toggle (`proof_2026-07-10_perprincipal_authz.log`). Token custody and per-tenant execution isolation run: two per-tenant Connect servers each inject only its tenant's catalog token server-side, a tenant-A session is refused on tenant-B `NotAuthorized`, and tenant-A/tenant-B run on disjoint executor pods (`proof_2026-07-10_multiserver.log`). Ingress routing is by identity: the Envoy gateway routes by client-cert URI-SAN, so a tenant-A cert reaches only tenant-A's server, an un-granted principal `403`, no-cert refused (`proof_2026-07-10_ingress_routing.log`). |
| **P6 multi-tenant scale** | multiple Connect servers + node autoscaling | Per-tenant Connect servers and gateway routing run today (2 tenants for the per-link isolation proofs; a third tenant was later provisioned by the same procedure for the §4 fleet capstone, S4.5); scaling out to many tenants with node autoscaling is the next step |

**Highest honestly-claimable today ≈ the full per-tenant isolation path, demonstrated link by link on EKS 2026-07-09/10: mTLS ingress routing by principal (Envoy routes by client-cert SAN) + token custody and per-tenant execution isolation (two per-tenant Connect servers, server-injected tokens, disjoint executor pods) + per-principal catalog authorization (Lakekeeper + OpenFGA/OIDC, including the direct cross-tenant vend deny) + data isolation at storage (credential scoping).** An agent authenticated as tenant A is routed to tenant-A's server, handed tenant-A's token, run on tenant-A's executors, authorized at the catalog only for tenant-A, and prefix-scoped at storage. What remains: the unbuilt *capability* is multi-tenant **scale** (many tenants + node autoscaling); two proof-completeness seams also remain, a single request composing all five links, and unifying the storage-scoping forensic onto the authorization-enabled catalog.

#### R: Reverse-engineering map (reference → as-built → SALVAGE / GAP)
| Component | Target | As-built | SALVAGE (keep) | GAP (build) |
|---|---|---|---|---|
| GitOps loop | agent→PR→CI gate→reconcile to prod | runs locally | `agent_pr_author.py`, `sdp_artifact.py`, dry-run + reconcile workflows, unit tests | wire to EKS Connect (not runner-local); capture a real agent PR; enable prod reconcile |
| Connect ingress | single governed mTLS endpoint + per-principal routing | built, with native client mTLS running: the Envoy gateway routes by cert-SAN to per-tenant servers | Envoy mTLS, principal interceptor, per-principal routing gateway, deployment, image | routing landed 2026-07-10; apply Terraform for the gateway; capture deploy artifacts |
| Elastic execution | driver + dyn executors, autoscaling | runs at small scale | dyn-alloc config, executor pod template, executor node group, image | prove 0→10→0 scale; add Karpenter/Cluster-Autoscaler |
| Catalog authz | per-tenant grants | Runs via Lakekeeper + OpenFGA + OIDC, denying per-principal at the catalog | governed Iceberg-REST catalog, per-tenant vending + grants, OIDC identities | delegation landed 2026-07-10; OSS HMS/Iceberg still cannot enforce, hence the governed catalog |
| Tenant exec isolation | per-tenant Connect / pools | Runs today: per-tenant Connect servers, gateway routing, and disjoint executor pods | per-tenant Connect servers, per-principal routing gateway, token injection, distinct executor pods | per-tenant servers and routing landed 2026-07-10; scale to N tenants + node autoscaling |
| Integration testing | structural + data-quality gate | structural only | the CI dry-run gate | data-quality / expectation tests in CI |
| Substrate (IaC) | reproducible cluster | terraform-applied + CI/OIDC-gated, live (2026-07-09) | terraform stack, k8s manifests, HMS, image | (apply DONE); keep capturing run evidence per deploy |

**Read R top-to-bottom to build: P0→P4 is mostly salvage + wiring; P5/P6 is genuine new construction (a governed catalog + multi-server Connect).**
