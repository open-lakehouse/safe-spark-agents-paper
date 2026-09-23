> **Safe, governed AI data engineering on Spark** · a working paper in four parts
>
> **The question.** Agents already write production data pipelines. The failure that actually hurts isn't a crash. It's a job that runs green and quietly ships wrong data. So: can you get the productivity without trusting the agent?
>
> **The answer, short.** **(1)** A controlled 528-run study: the same agent wrote pipelines two ways. Declarative (SDP), it had a structural dry-run that caught **79** defects before data moved, vs **0** for bare imperative — and because broken pipelines get rejected before executors start, imperative burned ~**34×** the cluster compute (~**1000×** on failed runs). **(2)** A dev loop built around what the agent can and can't touch: propose, gate, reconcile. The agent only ever emits an inert plan. It never holds a session, credentials, or data. **(3)** The platform that makes this real for many tenants: Spark Connect, Kubernetes, and a governed catalog, with all five per-tenant isolation layers live on EKS. Tenant A can't reach tenant B. **(4)** Omnigent, the fleet layer: one custodian holds every credential so no agent ever sees one. Demonstrated: one brief, one autonomous agent, three isolated tenants, governed medallion pipelines on the live platform. The quantitative fleet study is a separate paper.
>
> Each section carries its own maturity label. §1 and §3 hold the measured evidence. §4's core is demonstrated, and its numbers are deferred.

# SECTION 1: Imperative vs SDP
### A Safety-and-Cost Study of AI Agents Writing Spark Pipelines

## Abstract *(Section 1)*

The expensive failure isn't a crash. It's the silent defect: a pipeline that completes, passes its checks, and ships data that's quietly wrong. We asked whether the authoring paradigm changes how safely an agent writes Spark pipelines, and at what cost. One manipulation, everything else frozen: 22 tasks, 12 seeds, same model, same prompt, same decoding. Arm **A** wrote bare imperative PySpark. Arm **B** wrote Spark Declarative Pipelines (SDP) with the framework's built-in dry-run and a minimal API skill. 528 runs, 264 per arm. SDP's dry-run caught **79** structural defects before any data was processed; imperative caught **0** and met the same bugs at runtime. On the semantic residue no gate can see, the raw number made SDP look worse (0.326 vs 0.277 silent-defect rate). A controlled skill-swap traced that to one missing idiom, not the paradigm: teaching the skill how to bucket a UTC day took timezone defects from 7 to 0. SDP wrote about half the code (−49% lines, −44% AST) for ~2.3× the tokens, and because a dry-run rejection starts no executors, SDP can't burn cluster compute on a broken pipeline at all. Imperative used ~34× the total compute, ~1000× on failed attempts. Declarative structure buys a real, early safety margin on structural faults, and it doesn't make semantic faults worse as long as the skill actually teaches the paradigm.

## Introduction

Pipelines populate warehouses. They feed dashboards, reports, and numbers somebody eventually signs off on. A pipeline that throws an exception is one you fix. The dangerous one runs to completion and ships data that's quietly wrong: a dropped currency, a mis-bucketed day, a dedup that changes the numbers run to run. Nothing announces those. They surface downstream, long after the agent moved on.

Whether the agent writes them depends on more than the model. It depends on the paradigm it's asked to write in. With imperative PySpark the agent owns a live `SparkSession` and executes transformations directly. With SDP the agent declares the pipeline as desired state, a set of materialized views, and the framework assembles the graph and dry-runs it before data moves. The hypothesis: because SDP inspects the whole graph up front, it should catch a class of defects (unresolved columns, broken dependencies) that imperative only discovers at runtime, or never.

So we ran it, and this section reports: **79** structural defects caught at the gate vs **0**; a silent-defect gap that resolves to skill rather than paradigm; half the code at 2.3× the tokens; and ~34× more cluster compute burned by imperative. Root causes, operational definitions, and the pre-registered protocol live in [SUPPLEMENT-Section1.md](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/paper/SUPPLEMENT-Section1.md) as §SM1–§SM7.

## Reader's map

Terms, once:
- **Arm A**: bare imperative PySpark. No gate, no skill.
- **Arm B**: SDP + built-in structural dry-run + the `pyspark-sdp` skill (a 164-line API reference, not a safety aid).
- **Defect classes**: structural (D1 unresolved column · D4 broken DAG · D5 immutable-config mutation), semantic (D2 timestamp misparse · D6 nondeterministic dedup · D7 timezone/day-bucket · D8 silent row-drop), state (D3 unwatermarked dedup · D9 unbounded state, out of scope).
- **The gate / dry-run**: SDP validates the whole graph before data is touched. Imperative has no equivalent.
- **H1–H5**: safety, token cost, cluster compute, conciseness, efficacy.
- **N1 / N2**: LLM tokens vs data-processing compute, kept separate. **F1 / F2**: gate is intrinsic to SDP (kept) / bolt a gate onto imperative (rejected).
- **Cell**: one `(task, arm, seed)` run. The study has 528.

Platform terms arrive from §2 on: **Spark Connect** (session-less client/server front end; the client ships a plan, not code), **driver/executors** (planner vs worker pods), **mTLS** (identity from certificates, not claims), **IRSA** (keyless AWS auth from Kubernetes), **vend** (short-lived prefix-scoped credentials issued on demand), **reconcile** (a controller pushes infrastructure to match the declared spec), **Lakekeeper + OpenFGA** (governed Iceberg-REST catalog + Zanzibar-style authorization).

## Background

Imperative is what the base model knows natively: acquire a `SparkSession`, read inputs, transform, run. The agent is in full control and fully responsible. SDP inverts it. The agent writes decorated transformation functions, and the framework assembles the graph, resolves dependencies, and runs the pipeline. The agent never calls `.start()`, and in the governed setting later it never holds a session at all.

The property that matters is the dry-run. SDP resolves every view against the catalog and rejects structurally invalid pipelines — a column that doesn't exist, a view on a missing upstream table, an attempt to mutate immutable config — before a single executor touches data. Imperative PySpark has nothing like it, so the same faults become runtime exceptions after work already began. We treat the gate as intrinsic to the paradigm, not a bolted-on feature (F1, §1.4.2).

Defects split three ways (§SM3.2). Structural ones the gate can see. Semantic ones it can't, because the pipeline is well-formed and just computes the wrong answer. That semantic family is the silent-defect surface: the runs that complete and ship corruption. State defects need a live stream to manifest, so this study's offline oracle can't grade them; they're seeded in some tasks and left to future work. One consequence worth sitting with: a paradigm effect can appear only where the gate acts (structural defects), or in how well the agent was taught to handle semantic ones.

[[[SVG-TAXONOMY]]]

Arm B gets the `pyspark-sdp` skill because the base model barely knows SDP's newer API. It's a 164-line API reference that teaches mechanics only and, in its own words, says nothing about what your pipeline should compute. An earlier `spark-safety` skill was scrapped after it moved the silent-defect rate by 0.000. The one idiom this minimal skill happens not to teach — bucketing a UTC calendar day — is exactly what the residual gap in §1.4.1 traces to.

Safety, token, and code-size results are substrate-independent, so they ran local. The compute question (H3) needs both paradigms on one uniform cluster, so it ran separately on Spark Connect/EKS (§SM6.5). One model everywhere: `claude-opus-4-8`, identical decoding (§SM7.4).

## What we measure, and why

Safety and cost together, because a paradigm that's safer but slower to converge isn't automatically better. Five outcomes, pre-registered (§SM6.2):

- **H1, safety.** Where do failures get caught? At the gate (H1.1), the failure-mode distribution (H1.2), and the silent semantic residue no gate can see, kept as a control (H1.3).
- **H2, tokens.** How much LLM spend to a correct pipeline? SDP iterates more against its gate. We measure that instead of assuming it away.
- **H3, cluster compute.** How much executor time, especially on failures? A gate-rejected attempt processes zero data; a runtime failure already executed.
- **H4, conciseness.** Lines and AST nodes. The defensible half of the "less surface area" intuition.
- **H5, efficacy.** Does it finish the job? Read against H2 as cost-per-correct-completion: extra iterations are a win if they buy completion, a penalty only if they don't.

Two choices keep this trustworthy. N1 and N2 are separated because a gate changes them differently. And "silent defect" plus the detection stage are defined against the instrument code before anyone looked at results, so the endpoints can't be redefined to fit the data.

## Experimental setup at a glance

| | |
|---|---|
| **Manipulation** | one variable, the authoring paradigm. Arm **A** = bare imperative PySpark; Arm **B** = SDP + dry-run gate + `pyspark-sdp` skill |
| **Scale** | 22 frozen tasks (7 Low / 8 Med / 7 High) × 12 seeds × 2 arms = **528 runs**; 264 per arm, statistically powered |
| **Model** | `claude-opus-4-8`, identical decoding; **blind grading**: the grader sees output, never the fix |
| **Data** | deterministic synthetic event streams per (task, seed), with defect traps deliberately injected |
| **Substrate: safety / tokens / code** | local: imperative on local Spark, SDP on local Spark Connect |
| **Substrate: compute (H3)** | a real **EKS Spark Connect cluster**: client-mode driver pod + dynamically-allocated executor pods, Iceberg on S3, mTLS ingress |
| **Instrument** | frozen (`instrument-v3.2-frozen`); every reported number cites a committed results file |

The loop for one cell: generate the seeded data → agent proposes a pipeline → gate dry-runs the graph → execute → blind grade → record cost, repeat to a fixed iteration cap.

[[[SVG-RUNLOOP]]]

**Where the artifacts live**, in `open-lakehouse/safe-spark-agents-paper`:
- **Repro runbook** → [reproduce/REPRODUCE.md](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/reproduce/REPRODUCE.md)
- **Frozen corpus & seeds** → [study/config/TASKS.lock.json](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/config/TASKS.lock.json), [study/config/SEEDS.lock.json](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/config/SEEDS.lock.json)
- **Arms & config** → [study/arms/A.json](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/arms/A.json), [study/arms/B.json](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/arms/B.json), [study/config/study.config.json](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/config/study.config.json)
- **Harness & analysis** → [study/harness/](https://github.com/open-lakehouse/safe-spark-agents-paper/tree/main/study/harness), [study/analysis/analyze.py](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/analysis/analyze.py)
- **EKS compute run (H3)** → [study/repro/h3_eks/](https://github.com/open-lakehouse/safe-spark-agents-paper/tree/main/study/repro/h3_eks)
- **Results behind every number** → [study/results/results.powered.AB.n12.final.jsonl](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/results/results.powered.AB.n12.final.jsonl) (528 rows), [study/results/results.tzfix.jsonl](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/study/results/results.tzfix.jsonl) (the D7 skill-swap)

Links with line numbers pin the `paper-v1` tag so they resolve against the exact revision the numbers came from. The raw generated data and transcripts ship as `study/raw/sasa_raw_data_20260628.tar.gz`.

## 1.1 Research question

Does forcing an agent into Spark Declarative Pipelines instead of imperative PySpark produce safer pipelines, and at what cost? The manipulation is paradigm, only paradigm. The gate and the skill are part of the controlled environment, not the treatment.

## 1.2 Design: one manipulation

Everything a reader might suspect of driving the difference — model, tasks, seeds, prompt, decoding — is held fixed, so any difference is attributable to paradigm alone. One deliberate asymmetry: the structural gate stays with arm B, because it comes with declarative authoring and can't be given to imperative without making it something other than imperative. That asymmetry is the finding (F1; F2 in §1.4.2).

| Arm | Paradigm | Gate | Skill |
|---|---|---|---|
| **A** | imperative | none (bare) | none |
| **B** | SDP (declarative) | framework dry-run (built in) | `pyspark-sdp` |

Retired arms (imperative+gate+skill, SDP without skill) live in `study/arms/supplementary/`; the `spark-safety` skill was dropped everywhere after moving the silent-defect rate by 0.000 in pilot.

## 1.4 Results

Headline first, so it isn't missed: **SDP's dry-run catches 79 structural defects before any data is processed; bare imperative catches 0.** That's the load-bearing result. The rest of this section: the silent semantic residue around it (§1.4.1), gate validity (§1.4.2), cost (§1.4.3), and what each paradigm hallucinates (§1.4.4).

*Every number below comes from one powered run: 528 cells on the frozen instrument, 0 instrument-fault rows, mixed-effects logistic inference with Holm correction and bootstrap CIs. Recomputation spec: §SM6.*

### 1.4.1 Silent-defect rate: SDP looks worse until the skill swap

On the one thing a structural gate can't see, the raw powered run has arm B slightly higher. Don't stop at the raw number: this subsection shows the gap is one missing skill idiom, and it closes once the skill teaches it.

| arm | silent-defect rate | k/n | 95% CI |
|---|---|---|---|
| A (bare imperative) | 0.277 | 73/264 | [0.223, 0.330] |
| B (SDP) | **0.326** | 86/264 | [0.269, 0.383] |

Δ = −0.049 [−0.098, +0.000]; **OR = 1.97** (B vs A); GLMM p = 0.0033, Holm-adjusted.
[src: `study/results/results.powered.AB.n12.final.jsonl` · silent_defect · recompute §SM6]

**Which bugs carry the gap.**

| class | A ships | B ships | read |
|---|---|---|---|
| D2 timestamp misparse | 1 | 3 | negligible |
| D6 nondeterministic dedup | 38 | 39 | a wash, not SDP-specific |
| **D7 timezone / day-bucket** | **0** | **7** | **SDP-specific: imperative never ships it** |
| D8 silent row-drop / bad currency | 51 | 57 | B worse by +6, task-concentrated |

The whole gap is D7 (+7) and D8 (+6); the largest class, D6, is tied. A three-agent code audit found the mechanism (full forensics in §SM1): SDP's immutable-config rule removes the one-line `session.timeZone = UTC` fix imperative can use, and the base skill was silent on the replacement idiom. So the controlled skill-swap taught arm B the UTC day-bucketing idiom and re-ran: **D7 went from 7 to 0** (`study/results/results.tzfix.jsonl`), matching imperative. The residue is skill-induced, not paradigm-inherent. Structure isn't unsafe. It needs a skill that teaches the paradigm-matched idiom, and then the arms reach parity. (A 3-seed pilot pointed the same way: A 18/66, B 23/66.)

[[[SVG-COMPOSITION]]]

### 1.4.2 Structural defects at the gate

Where structural defects (D1/D4/D5) get caught — defect-level, across all iterations; a gate-caught-then-fixed bug still counts:

| arm | at gate (dry-run) | at runtime | shipped |
|---|---|---|---|
| A (bare, no gate) | 0 | 4 | 0 |
| B (SDP) | **79** | 30 | 0 |

SDP's framework dry-run intercepted 79 structural defects (353 iteration-level error events) before any data moved. Bare imperative has no gate, so its 4 structural catches surface at runtime after compute was spent. The gate wasn't a harness gift: it's what declarative pipelines do by construction. Bolting an artificial gate onto imperative (F2) was explicitly rejected — it would contaminate imperative with a feature it would never naturally have. The retired A2 arm's gate audit and design history are in §SM2.

[[[SVG-WHERE]]]

### 1.4.3 Cost: half the code, 2.3× the tokens, and a structural compute asymmetry

**Conciseness (H4).** Paired over (task, seed) on the final accepted program (Δ = A − B, positive means B wrote less; `*_body` excludes scaffolding):

| metric | B (SDP) | A (imperative) | Δ (A−B) | 95% CI |
|---|---|---|---|---|
| final_program_loc | 67.9 | 134.0 | +66.1 | [+61.9, +70.4] |
| ast_node_count | 614.9 | 1105.5 | +490.6 | [+453, +530] |

All CIs clear of zero: B is ~49% fewer LOC and ~44% smaller AST. [src: `study/results/results.powered.AB.n12.final.jsonl` · final_program_loc / ast_node_count]

**Tokens (N1).** Medians to a correct pipeline, both arms fully populated (264/264):

| arm | input | output | total | vs A |
|---|---|---|---|---|
| A | 1,436 | 9,964 | 11,524 | 1.0× |
| B | 7,295 | 18,499 | **26,480** | **~2.3×** |

SDP iterates more against its gate, and that shows up as tokens. [src: same results file · input_tokens / output_tokens · per arm]

**Cluster compute (N2), the sharpest cost result.** This one isn't a matter of degree. It's structural. Imperative runs `spark-submit`, spends executor time, and only then discovers the pipeline is wrong. SDP's dry-run rejects a structurally-invalid pipeline before any executor starts, so a failed SDP attempt costs ≈ 0 cluster compute by construction. **69.5% of arm B's attempts were intercepted at the gate, before touching data.** Scope note: a semantically-wrong SDP pipeline passes the dry-run and burns compute like imperative — that's the un-gateable residue of §1.4.1.

Measured on one uniform EKS Connect cluster, imperative spent ~**34×** the total executor-seconds and roughly **1000×** on failed attempts.

Dollar version (a projection from the measured mechanism, not a measured result): a real pipeline over ~100 GB whose failed attempt burns ~10 minutes across a 20-executor cluster wastes ≈ **$0.60 per failed attempt**. An imperative agent that fails ~2× before converging wastes ≈ $1.20 per pipeline, so at ~1,000 pipelines/week that's ≈ **$5,000/month** of cluster compute SDP never spends. Against that, SDP's ~15k extra tokens per pipeline cost ≈ $0.73 at Opus-class rates (~$15/$75 per million input/output tokens), ≈ **$3,000/month** at the same scale. Big pipelines favor the gate; small ones flip. All of it recomputes from the token deltas above.

[[[SVG-WASTE]]]

[[[SVG-COST]]]

### 1.4.4 What each paradigm hallucinates

Beyond whether an agent fails, what it invents differs sharply by paradigm, and the difference maps onto where the gate can act. (A qualitative characterization over the exploratory sweeps, not a powered magnitude. "Hallucination" = inventing something that doesn't exist, or writing code for the wrong paradigm; §1.4.1's silent defects are a separate axis.)

| hallucination | imperative (A) | SDP (B) |
|---|---|---|
| invents an I/O path (nonexistent input/output location) | **51** | 0 |
| imperative session control (`spark.conf.set`) inside a declarative pipeline | 0 | **76** |
| invented / undeclared table or view | 1 | **40** |
| eager action (`.collect()`) inside a declarative query function | 0 | **27** |
| invented column name | 3 | **21** |
| invented / unsupported API | **4** | 0 |

Imperative invents where the data lives: a hard-coded path that never resolves. Nothing structural can know a path is wrong until storage is touched, so **96%** of imperative error-iterations surface at runtime and the agent loops re-guessing — a direct source of the wasted compute in §1.4.3. SDP's hallucinations are imperative habits leaking into a declarative frame, and the gate eats them: 80 of 96 `spark.conf.set` occurrences were rejected at the dry-run, **39%** of SDP error-iterations die cheaply at the gate vs 3% for imperative. The same `spark.conf.set` line is legitimate in imperative (63 final programs keep it, no error): an identical keystroke, caught as hallucination in one paradigm and correct in the other. Neither arm hallucinated Databricks DLT (`import dlt`, `@dlt.table`): **0 in both** — the SDP arm's governed skill keeps it on the OSS API. [src: `study/raw/raw_20260628/all_results.jsonl` · per_iteration error_class × arm × stage]

[[[SVG-HALLUCINATION]]]

## 1.5 Threats to validity

- **Gate asymmetry.** Arm A is bare by design, so the 79-vs-0 contrast measures each paradigm as it natively is. No gate-rigor confound.
- **Substrate split.** Imperative ran on local Spark, SDP on Connect. That blocks a fair executor-seconds comparison, so the compute claim (H3) is measured separately on one uniform EKS cluster (§1.4.3).
- **Sample size.** The powered run is complete: 264 cells/arm ≥ 260 required, 0 instrument-fault rows. The silent-defect endpoint came back informative rather than null (OR 1.97, p = 0.0033), which the skill-attribution then explains.
- **Token instrumentation.** Fully populated both arms (264/264); B ≈ 2.3× A.

## 1.8 Conclusions

The study isolates the paradigm, and the safety result is clean. SDP's dry-run intercepted 79 structural defects before any executor started; bare imperative met the same faults only at runtime. That gate isn't a gift we handed SDP — it's a property declarative authoring has by construction and imperative can't have without ceasing to be imperative.

The counter-signal gets reported without softening. On semantic residue, SDP is slightly worse in the raw data (0.326 vs 0.277; OR 1.97, p = 0.0033). The gap is mostly timezone/day-bucket bugs carried by a missing skill idiom, and teaching it closed D7 **7 → 0**. The honest headline isn't "structure is unsafe". It's that structure alone isn't enough: it needs a skill that teaches the paradigm-matched idiom, and once it has one, the arms reach parity.

Cost is coherent. SDP wrote ~half the code for ~2.3× the tokens while completing at a comparable rate (65.2% vs 68.9%, a gap that tracks the skill-attributable D7 residue). Whether the token premium is worth an early structural margin and half the code is a judgment call; the study makes the inputs explicit either way. Two limits bound the claims: the compute comparison requires one uniform cluster (§1.4.3, §SM6.5), and the study fixes a single model while giving B an API skill A doesn't need.

The safety result motivates what's next. If the valuable thing about declarative authoring is catching faults before data moves, can we build a system where the agent never holds a live session at all? That's Section 2.

---

> **§1 methods.** The methods appendix is a separate file, [SUPPLEMENT-Section1.md](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/main/paper/SUPPLEMENT-Section1.md): root-cause forensics (§SM1), gate history and retired arms (§SM2), operational definitions (§SM3), the pre-registered run protocol (§SM6), full materials and system (§SM7).

# SECTION 2: The Agent-Native Development Loop

## Abstract *(Section 2)*

§1 showed a declarative agent writes safer, shorter pipelines. This section shows why, and turns it into a loop. The agent's whole job is to declare the tables and stop. No session to open, no endpoint to point at, no credential to hold, no cluster to configure — so the loop can be built around a fallible, untrusted author. The agent proposes inert desired state; a gate rejects bad structure up front; a controller the agent doesn't control holds the credentials and reconciles. Authoring and execution get separated by construction, and that separation is what lets the same agent run fully untrusted. We built it and ran real agents through it, end to end and across hosts on a live EKS cluster.

## Introduction

One developer, one pipeline. §2 walks the loop step by step: why run-to-debug is wrong for an agent (§2.1), what the agent writes (§2.2), the loop itself (§2.3), what it buys (§2.4), dev vs prod (§2.5), and where it honestly stands (§2.6). The multi-tenant, production-grade version of this boundary is §3's problem.

## 2.1 The problem: building a pipeline by running it is wrong for an agent

The normal way to build a Spark pipeline is imperative: write code, run it, learn what's wrong from what breaks. Every mistake costs a run. That loop was designed for a trusted human at a keyboard, and it assumes the author holds a live session, warehouse credentials, and a cluster endpoint. An untrusted agent can safely be given none of those.

An agent holding a `SparkSession` is a governance problem with no gate before data, no line between what it authors and what it runs, and nothing to audit or contain. It can mutate config, read or overwrite arbitrary tables, burn compute on pipelines that never worked, or ship §1's silently-wrong data. This section shows the alternative: give the agent a full production platform without ever handing it the runtime keys.

[[[SVG-DEVLOOP]]]

## 2.2 What the agent writes

One property does all the work: the agent authors an inert description of desired state and can touch nothing else. The two paradigms make fundamentally different artifacts.

```python
# Imperative: the agent owns and runs the session; authoring IS execution.
spark = SparkSession.builder.getOrCreate()
orders = spark.read.parquet("s3a://warehouse/orders/")
orders.filter(...).write.saveAsTable("orders_clean")   # runs live data ops now

# Declarative (SDP): the agent writes only inert desired-state; the framework runs it.
@dp.materialized_view
def orders_clean():
    return dp.read_table("orders").filter(...)          # a declaration, nothing executes
```

## 2.3 The dev loop, step by step

Author → PR → gate → reconcile. The agent writes the transform and the spec and opens a PR. CI dry-runs the changed specs against the real catalog, so a structural bug is rejected before merge, before any data moves: a spec with a missing upstream fails the gate with `[TABLE_OR_VIEW_NOT_FOUND] … SQLSTATE 42P01`, a valid one dry-runs to `Run is COMPLETED`. On merge, the reconciler — never the agent — runs it over Spark Connect. The agent that authored it holds no credentials and never ran it. That's review plus a real integration gate plus controller-owned execution plus a full audit trail, with the agent strictly outside the runtime.

[[[SVG-CONTROLBOUNDARY]]]

[[[SVG-GITOPS-LOOP]]]

Honest seams, named up front: the loop runs locally and the gate targets runner-local Connect, not the EKS endpoint (`study/gitops_demo/PRODUCTION_EKS.md`). No captured run of a real agent-opened PR exists in public. Wiring the CI controller's identity to the per-tenant runtime certificate is design-only. And the gate checks structure: data-quality/expectation tests — the natural place to catch §1's silent semantic bugs — aren't built.

## 2.4 What this buys

The agent's surface is declare-the-tables, so there's no session lifecycle, endpoint, or credential to get right, and effort goes to the transform instead of plumbing. That's part of why it writes ~half the code of imperative (§1.4.3) and still completes (§1.4, H5). The containment properties follow. It's untrusted by design: no session, no credentials, nothing to leak. Structural mistakes die at the gate (79, §1.4.2) instead of at runtime. A broken pipeline can't burn cluster compute, because it never runs (§1.4.3). And the output is an inert, reviewable artifact, so the loop audits cleanly and — since nothing the agent writes is ever run by the agent — scales to many tenants (§3). The price: ~2.3× the tokens from iterating against the gate. Cheap model calls to save cluster compute, wrong data, and blast radius. For an untrusted author on production data, that's the trade you want.

## 2.5 Develop locally, promote by changing one endpoint

The controller is a plan-builder pointed at a URL, so the same agent, spec, and gate run against a local Connect server while iterating — fast and free — and promote to the EKS cluster by changing one variable, `SPARK_REMOTE`, with no code change. The swap happens entirely outside the agent's surface: the agent authors one inert spec, byte-identical in dev and prod, and never sees or sets the variable. Where the pipeline runs isn't the agent's concern, and can't be.

[[[SVG-DEVPROD]]]

There's a precise reason SDP keeps this promise where imperative only mostly does. Connect refuses to hand a client the engine: `SparkContext`, the JVM, RDDs, and static cluster config stay server-side and error if you reach for them (`JVM_ATTRIBUTE_NOT_SUPPORTED`, `CANNOT_CONFIGURE_SPARK_CONNECT_MASTER`). So there's a privilege ladder: the cluster admin owns the engine, the controller may submit plans but never touch it, and the agent holds not even a session. SDP sits on the bottom rung by construction; imperative runs over Connect too, but agents routinely reach for `sparkContext` or `_jvm`, so imperative only happens to stay in bounds when Connect stops it.

[[[SVG-CONNECT-LADDER]]]

## 2.6 Where this stands, and what §3 takes on

This ran for real: a live agent (`claude-opus-4-8`) over §1's full corpus, and separately across hosts on a real EKS Connect cluster (`ssa-spark-eks`), where the controller submitted over Connect, driver and executors ran in Kubernetes pods, and SDP pipelines materialized to Iceberg on S3. Authoring and execution were genuinely separate machines. Two gaps are named so nothing reads as more finished than it is: those remote runs reached Connect through a `socat` tunnel instead of native mTLS, and the reconciler still runs alongside the agent rather than split onto the platform ([`live.py:675-689`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/study/harness/backends/live.py#L675-L689)). Neither changes the boundary. The invariants and layer-by-layer build state are Appendix S2-A.

§3 is what an infrastructure engineer stands up to make this boundary real and multi-tenant: governed catalog, Kubernetes execution, per-tenant credential vending, and the trust model that keeps one agent's blast radius off every other tenant.

---

# SECTION 3: The Open Reference Architecture

## Abstract *(Section 3)*

§2's boundary, seen from the infrastructure side. §2 gave one developer a loop where the agent's only artifact is an inert PR; §3 takes that as given and stands up the platform underneath it, adding four pieces §2 didn't have: a governed **catalog** (Iceberg-REST, demonstrated on Lakekeeper + OpenFGA), **Kubernetes** execution (a client-mode Connect driver spawning executor pods), **credential vending** (the catalog mints short-lived, prefix-scoped credentials, never the agent), and per-tenant **trust**. The center of gravity is a five-layer isolation stack — L1 ingress routing, L2 token custody, L3 execution isolation, L4 catalog authorization, L5 storage scoping — whose one job is to make it impossible for a tenant-A agent to reach tenant-B's data by any of five paths. All five run on a live EKS cluster today. The one capability still ahead is multi-tenant scale.

## Introduction

From the infrastructure engineer's seat, §3 is a separation of concerns: each layer owns exactly one thing and delegates the rest (§3.0). **§3.1** is the GitOps/CI boundary that tests and reconciles every change; **§3.2** is Connect-on-Kubernetes, the governed ingress and the elastic execution behind it; **§3.3 is the section's core**, the five-layer isolation stack, walked link by link; **§3.4** states where each pillar stands; **§3.5** hands credential custody at fleet scale to §4/Omnigent.

Because §2's agent only ever emits inert desired state, it can be dropped into a governed platform that trusts it with nothing: **SDP** for declarative authoring, a **GitOps/CI** layer that tests and reconciles every change, **Spark Connect** as the single identity-pinned front door, and **Kubernetes** for elastic execution. Tenant A is routed to *its own* Connect server, handed a credential it never holds, run on *its own* executor pods, authorized at the catalog only for itself, and prefix-scoped at storage — no path to tenant B. The five links were re-verified fresh on 2026-07-14 (`paper/notes/proof_2026-07-14_live_isolation.log`); §3.4 maps where each pillar stands, and its Frontier row states what's still ahead.

[[[SVG-SECTION3]]]

---

## 3.0 One layer, one job

| Layer | Owns | Delegates / does NOT own |
|---|---|---|
| **SDP** | declarative authoring: *what* the pipeline is | execution, identity, tenant authz |
| **GitOps / CI** | continuous test + integration against a real target catalog; reconciliation | defining the pipeline; enforcing grants |
| **Spark Connect** | the single governed, identity-pinned **ingress** | the data work itself (executors) |
| **Kubernetes** | elastic execution: client-mode driver + executor pods | the governance boundary (stays at the ingress) |
| **Catalog** *(governed Iceberg-REST; Lakekeeper + OpenFGA, vendor-neutral)* | per-principal authorization, grants, credential vending | being reinvented by SDP or GitOps |

## 3.1 GitOps / CI: tested integration, not blind submission

The naive setup lets an agent submit code straight to a cluster: a live session in the agent's hands, untested and unreviewed changes in prod, no gate, no audit. The fix: the agent's only output is a PR. CI triggers on any PR touching `pipeline-definitions/**`, stands up a real Connect server, resolves the changed specs, and dry-runs each against actual catalog and schema state. Broken DAGs and missing upstream tables are caught before merge, before any data moves. On merge, `reconcile.py` does controller-owned execution. The agent that authored it holds no credentials and never ran it.

Honest scoping: the full author→PR→gate→reconcile loop runs locally against runner-local Connect; the production-EKS GitOps path is documented but not wired (`study/gitops_demo/PRODUCTION_EKS.md`). No public run of a real agent-opened PR is captured. The authoring→runtime bridge — the CI controller presenting a per-tenant client cert (SAN `spiffe://safe-spark-agents/tenant_a`) to the gateway — is design-only, named here rather than left implicit. And the gate is structural: data-quality/expectation tests, the obvious extension, aren't built.

## 3.2 Connect on Kubernetes: Connect is the front door, k8s is the horsepower

Spark Connect is Spark's session-less front end: a client sends a query plan to a shared server, never code to execute. The driver plans and coordinates; executor pods do the work. Scale happens behind the governed door, never around it.

The boundary lives in three facts. One external port, mTLS only: TCP 15009 is the sole externally reachable endpoint; raw Spark Connect (15002) is never exposed. Envoy terminates that mTLS, reads identity from the client certificate — discarding any identity the client tried to assert — and stamps the verified principal on the request. And the Connect server listens only on loopback, so the proxy is the only way in: every session arrives with a cryptographically pinned principal no matter how execution scales behind it.

Driver + executors: the Connect pod is a client-mode driver (`spark.master=k8s://…`, `deployMode=client`) that creates executor pods through in-cluster RBAC scoped to its own namespace, pinned via a pod template to executor-labeled nodes with topology spread. Dynamic allocation is configured 0→10.

[[[SVG-CONNECT-K8S]]]

What actually ran: a real EKS cluster, one shared long-lived driver (a singleton, `replicas:1` — Connect sessions are server-local), and a `spark.range(80_000_000).sum()` probe that registered real Spark stages on the cluster's executor pods, not the local process (executor-seconds 1.246, cpu-seconds 0.878). That confirms execution genuinely left the client. It's a small probe, not a parallelism benchmark. The substrate is terraform-applied and CI/OIDC-gated as of 2026-07-09, with no long-lived AWS keys; elastic 0→10→0 and node autoscaling (Karpenter) are configured, not reproduced.

## 3.3 Tenant governance: the multi-tenancy stack, built and demonstrated

Why co-tenant at all instead of one cluster per tenant? Consolidation — one elastic compute pool and one governed catalog beat a cluster per team on cost and ops surface. But consolidation is exactly what puts a hostile or hallucinating tenant-A agent one misconfiguration from tenant-B's data, so co-tenancy has to be safe by construction, not by trusting the agent.

The tenancy model, decided before any mechanism:

| Model | Shape | L1/L2/L3 | L4/L5 | Scales? |
|---|---|---|---|---|
| **A** · server per tenant *(shown below)* | one server + driver + executors per tenant | static token; disjoint pods (physical) | per-principal | no |
| **B** · shared servers | tenants multiplex a few servers | per-request pin + vend; shared executors (logical) | per-principal | yes |
| **C** · hybrid *(chosen)* | shared by default + optional dedicated tier | per-request pin + vend; shared, physical optional | per-principal | yes |

Chosen C because of a §2 property. The risk of a shared executor depends on what runs there: an imperative agent could scavenge a co-resident tenant's in-memory or shuffle data, but an SDP agent emits inert declarative transforms that the framework runs — never agent code. Shared execution is safe for declarative agents in a way it isn't for imperative ones, so physical separation drops from requirement to optional defense-in-depth. Two more properties make this production-shaped rather than a lab convenience: a Connect server pool needn't be homogeneous (it can span AWS, on-prem, Databricks, routing to the best fit), and the pressure moves off Connect onto the catalog — a metastore that owns metadata, authz, and vending, not the write path. Catalog-side scale is itself left to future work.

What's proven vs frontier: the mechanism below demonstrates on **Model A**, the strongest-isolation end of the spectrum where every layer is physically separate. The hybrid headline — two tenants sharing one Connect server who still can't touch each other — is the proof being re-earned on the EKS-native platform, named as frontier rather than claimed.

**The adversary, and the five paths.** The threat is §2's: a tenant-A agent that is fully untrusted — it may emit code that executes on the cluster, hallucinate, or actively try to reach tenant B's data. Reaching tenant B decomposes into exactly five paths, the ledger below. Each is closed by a different layer, each layer is independently defeatable, so all five must hold. And the later layers can't backstop failure of the earlier ones: a routing or custody failure produces a legitimately issued tenant-B identity, not a forgery, which the catalog and storage gates would then correctly serve. The early links keep the identity honest; the late links bound what an honest identity may do. That's why the chain is non-redundant.

[[[SVG-ADVERSARY]]]

| Path | Layer | Enforced by | Blast radius if this layer alone fails |
|---|---|---|---|
| connect | **L1** ingress routing | route by the certificate's authenticated principal | a tenant-A cert reaches tenant-B's server |
| be handed | **L2** token custody | inject only the tenant's own token, never client-held | a session presents tenant-B's credential |
| execute | **L3** execution isolation | own pods, no shared JVM | a co-resident session reads tenant-B's in-process data |
| ask catalog | **L4** catalog authz | authorize every request, including the vend | an agent is handed tenant-B's tables or credential |
| hit storage | **L5** storage scoping | downscope the vended credential to the tenant's prefix | a credential reaches tenant-B's bytes |

**The default open stack stops at identity.** Envoy pins an unspoofable principal from the client-cert SAN, and the interceptor rejects a mismatched `user_id`. The platform knows who each session is. But authorization stays fleet-scoped: one shared Iceberg catalog and one fleet-wide IAM role (IRSA) with read/write to the whole warehouse. Per-principal schema isolation exists by convention, not enforcement, and the stock open-source catalog can't express per-user grants at all — audit and fleet-wide grants only, no per-user grants, no row/column masking. Execution is shared too: one long-lived driver, shared executors, no per-tenant pool. That's the baseline the five links close.

[[[SVG-ISOLATION]]]

**The five links, outside in.** Each is demonstrated on live EKS (2026-07-10 unless noted), each names the residual threat it alone closes and the attack that still works with that layer removed but the other four intact. The interactive figure `paper/figures/isolation-architecture.html` renders the whole path.

1. **The gateway routes by who you are, not what you ask for.** Envoy terminates the client's mTLS, derives the principal from the certificate's URI-SAN (`spiffe://safe-spark-agents/<tenant>`), and routes on that. A tenant-A cert reaches only `spark-connect-tenant-a`; tenant-B only its own server; an un-granted principal gets **403**; no cert never clears TLS. The authenticated identity, not the client's choice, selects the server, so no route can send a tenant-A cert to tenant-B's server. (`paper/notes/proof_2026-07-10_ingress_routing.log`)
2. **A tenant only ever holds its own token, and never sees it.** Each tenant's Connect server injects its own catalog token in config, never exposed to the client. A session on tenant-A's server operates only as tenant-A; configuring tenant-B, it holds no credential and is refused (`NotAuthorized: Missing Authorization Header`). No token to redirect or replay. (`paper/notes/proof_2026-07-10_multiserver.log`) Full custody — holding and rotating the vended credential — is §4/Omnigent's job; §3 shows the per-tenant server binding.
3. **Each tenant's work runs on its own pods.** Connect sessions are server-local, so tenant-A runs on its own driver's executor pods (2 pods, distinct IPs), tenant-B on another driver app and pods. Never a shared JVM: no in-memory or shuffle reads across tenants, no scavenged process credential. (`paper/notes/proof_2026-07-10_multiserver.log`)
4. **The catalog authorizes every request per principal (Lakekeeper + OpenFGA).** A tenant-A identity is denied at the catalog for tenant-B across warehouse resolution and namespace ops (`404`, existence hidden for a zero-relation principal); tenant-B and admin get `200`, unauthenticated gets `401`. Toggling a `describe` grant flips `404`↔`200`, proving it's authorization, not nonexistence. The vend path is probed directly, not inferred: tenant-B's own `loadTable` carrying the `vended-credentials` delegation returns `200` **with credentials**, while tenant-A gets `404` and nothing is vended. (`paper/notes/proof_2026-07-10_perprincipal_authz.log`, sections A–D)
5. **Even a leaked credential reaches only its own prefix.** The catalog vends keylessly — IRSA assumes a downscoping role, external-id-pinned — per tenant prefix. Replaying the vended credential against the other tenant's prefix is `AccessDenied` in both directions; an ablation confirms a whole-bucket credential would cross, so the deny is the downscoping vend, not the base policy. CloudTrail settles that compute uses the vend and nothing else: every warehouse object call rides the vended session, and the fleet IRSA role makes **zero** data calls. (`paper/notes/cloudtrail_vend_evidence.md`, `paper/notes/proof_2026-07-10_delta_and_frontier.log`)

The vend is configured, not narrated — each tenant's storage profile names its own prefix, `sts-enabled`, and the external-id-pinned vending role ([`deploy/eks/terraform/lakekeeper-vending.tf`](https://github.com/open-lakehouse/safe-spark-agents-paper/blob/paper-v1/deploy/eks/terraform/lakekeeper-vending.tf)):

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

Implementation notes for link 1: over gRPC the no-route deny surfaces as an HTTP-200 carrying `x-routed-to=DENIED` and `grpc-message: no tenant route` (plain HTTP gets a real 403). Two server-side checks bar dialing in around the gateway: the PSK bearer the gateway injects (an infrastructure secret, not per-tenant) and the principal-pinning interceptor (rejects a `user_id` that doesn't match the gateway-derived `x-connect-principal`). The per-tenant servers are ClusterIP-only but bind `0.0.0.0:15002`, so those two secrets — not topology — are today's door; a `NetworkPolicy` is shipped as defense-in-depth but not yet applied.

**Proven link by link, not one composed request.** Each link runs on live EKS, and links 2–5 already compose: a write through tenant-A's server draws a tenant-scoped vend from the authorization-enabled catalog and runs on tenant-A's executors. Two honest seams remain: the storage-scoping forensic was captured against the fleet-scoped catalog while routing, custody, execution, and authorization ran against the authorization-enabled one; and a single request traversing all five links hasn't been captured as one job. Both are named so nobody reads a composed run that didn't happen.

**Vend vs custody, the §3↔§4 line.** The catalog vends short-lived, scoped credentials but never hands them to an agent. Holding and managing the vended credential — custody plus the agent interface — is the orchestration layer's job (§4/Omnigent). §3 owns the catalog as grant authority and vendor; §4 owns custody.

[[[SVG-CUSTODY]]]

**Honest scoping, per link.** Two measurement caveats the per-link proofs carry, stated so they aren't glossed over. The storage-scoping probe (a per-write delta of 12 tasks around an 8-partition shuffle under `spark.master=k8s`) proves only that execution left the driver onto a dedicated executor pod — and on that shared-server run both tenants landed on the same pod, so per-tenant pod disjointness is the separate multi-server result (link 3, distinct pod IPs). And the cross-tenant storage denial is observed by replaying the vended credential against the other prefix, not by an executor being refused in-cluster; the executor-side channel shows only that all FileIO used the vend (CloudTrail: fleet IRSA, zero data calls). Full build + proof narrative: `paper/notes/PLATFORM_LAB_NOTEBOOK.md`.

### Catalog binding: Lakekeeper and Unity Catalog OSS

*The governed catalog is a swappable slot, not a Lakekeeper dependency. (Disclosure: a co-author works on open-source Unity Catalog at Databricks; this evaluation names UC OSS's gaps as plainly as its strengths, and every UC claim is verified against v0.5.0 source or reproduced live.)*

L1 ingress routing, L2 token custody, and L3 execution isolation are catalog-independent — they live at the ingress, the per-tenant servers, and the executor pods, and carry a Unity Catalog token exactly as they carry a Lakekeeper one. L4 per-principal authorization and L5 prefix-scoped vending are the catalog's job, and both bindings enforce them.

- **Lakekeeper + OpenFGA (Iceberg-REST)** — the primary binding: per-principal authz via Zanzibar relationship checks, cross-tenant catalog request `404`, vend refused for an un-granted tenant, STS creds downscoped and external-id-pinned. One vendor-neutral Rust binary, natively Iceberg-REST.
- **Unity Catalog OSS 0.5.0 (native plugin)** — verified against source and reproduced live: per-principal authz via JCasbin RBAC, the vend endpoint gated by the same grants (`@AuthorizeExpression(VEND_TABLE_CREDENTIAL)`: `READ`→`SELECT`, `READ_WRITE`→`SELECT`+`MODIFY`), 1-hour STS `AssumeRole` creds inline-policy-scoped to the exact prefix with `sts:ExternalId` pinning. Live AWS, **6/6 checks** (`paper/notes/proof_2026-07-10_uc_vending.log`): tenant A vends real downscoped `ASIA*` creds for its own prefix; the same principal is refused `PERMISSION_DENIED` for tenant B's location (both directions); the replayed credential yields S3 `AccessDenied`. And end to end through Spark, the UC-native connector carrying a per-tenant UC token: tenant A reads only its Delta table via `loadTable` authorization, then vends, then reads the tenant's Delta files in S3 — and is refused tenant B's table at the catalog (`PERMISSION_DENIED`), both directions.

Where Unity Catalog OSS is genuinely weaker (and it is), stamped to v0.5.0:

| Gap | Kind | Detail |
|---|---|---|
| Iceberg-REST path can't do per-principal L4 | **hard** | every Iceberg-REST route authorizes at metastore-`OWNER`, read-only/UniForm-only; per-principal only via the native plugin |
| Authorization off by default | posture | ships `server.authorization=disable`; isolation is opt-in, not default |
| No RLS / column masking / ABAC | **hard** | coarse object-level RBAC only |
| RBAC, not relationship-based | design | JCasbin ACL/RBAC vs OpenFGA (Zanzibar); single, non-thread-safe on concurrent policy reload |
| Identity mapping | setup | maps OIDC `email` to a pre-provisioned user (no default JIT); per-user grant work per tenant |
| Authz maturity | maturity | standing up per-principal v0.5.0 needed two authz bug fixes |
| Operational weight | ops | server + Postgres + OIDC token-exchange + JCasbin vs one Rust binary |

**Takeaway.** Per-principal authorization and prefix-scoped vending — the two things §3's isolation rests on — are achievable on both bindings. The full isolation proof demonstrates on Lakekeeper; the catalog-integrated path (token custody, per-principal authz, downscoped vending, executor data-read isolation) reproduces end to end through Spark on UC OSS, with UC's weaker spots named above so the choice is informed.

## 3.4 Where each pillar stands

| Pillar | Running on live EKS | Configured but not yet run | Still ahead |
|---|---|---|---|
| GitOps/CI | PR-author session denial; dry-run + reconcile workflows | EKS-target reconcile | (none) |
| Connect-on-k8s | topology; small-scale distributed exec | elastic 0→10 executors | node autoscaler |
| Tenant governance | **per-principal mTLS ingress routing** (Envoy routes by cert SAN); **token custody + execution isolation** (two per-tenant servers, server-injected tokens, disjoint pods); **per-principal catalog authz** (Lakekeeper+OpenFGA+OIDC, both directions, grants toggle); **per-tenant storage isolation** (vended creds, cross-tenant `AccessDenied`, fleet IRSA 0 data calls) | (none) | multi-tenant scale (many tenants + node autoscaling) |

**Frontier.** Three things: multi-tenant scale — many tenants and node autoscaling, the one unbuilt capability; a single request composing all five links end to end; and unifying the storage-scoping forensic onto the authorization-enabled catalog. Two design-only seams sit under GitOps: the gate targets runner-local Connect (§3.1), and the controller's identity isn't wired to the per-tenant runtime certificate (§3.3). Everything else in this section runs on the live cluster.

**Reproduce it:** `deploy/eks/lakekeeper/SETUP.md`, four sub-deployments built inside-out, each writing the proof log cited above.

[[[SVG-REPRODUCE]]]

## 3.5 Where §3 hands off

§3 governs one agent — routing, custody, execution, authorization, storage — per tenant, live on EKS. That governed substrate is the precondition for orchestrating a whole fleet of agents, which is §4.

---

# SECTION 4, Omnigent: Governed Multi-Agent Orchestration for Data Engineering
### An orchestration layer for a fleet of governed agents

**The thesis.** §3 governs one agent safely. **Omnigent** — an orchestration layer enacted at runtime by one orchestrator driving credential-free sub-agent workers — governs a fleet. A fleet isn't "more agents in parallel"; an orchestration layer makes many data-engineering agents cheaper, higher quality, governed, and collectively knowledgeable in ways N independent sessions can't be by construction, having no shared coordination layer. The load-bearing one is governance: one custodian holds every per-tenant credential from §3 and enforces each tenant's contextual data policy at submit time, so the fleet stays credential-free and policy-bound, §2's boundary preserved at fleet scale. The keystone and the orchestration pattern run in the capstone below; the quantitative cost/quality numbers are the separate pre-registered study (S4.6).

- **S4.1 Cost: heterogeneous model routing.** Match the model to the task — cheap for a trivial fix, strong for a refactor, a different vendor for review. Metric: cost-per-correct-pipeline, §1's H5.3 lifted to the fleet. Number deferred to S4.6.
- **S4.2 Quality: cross-vendor review.** A different-vendor reviewer catches defects a correlated-blind-spot same-vendor review structurally misses. A testable catch-rate hypothesis; number deferred to S4.6.
- **S4.3 Governance: credential custody (the keystone).** The catalog vends short-lived scoped credentials; Omnigent holds custody and mediates the agent-to-catalog interface so no agent ever sees a credential. In a 2026-07-10 run, one custodian governed both tenants, minted and rotated three short-lived (300s) credentials with the agents holding none, and an attempted cross-tenant read was refused (`PERMISSION_DENIED`) (`paper/notes/proof_2026-07-10_sp41_custody.log`). Contextual data policy — quarantine, PII masking, value conservation — goes through the same gate, enforced at submit time.
- **S4.4 Knowledge: shared skill library.** One governed, versioned skill library injected fleet-wide gives correctness propagation, consistency, single-point updates, and a guaranteed floor. Load-bearing detail: §1 measured 0 Databricks-DLT hallucination in both arms with the skill equipped; the without-skill default (agents reaching for DLT) is a documented ablation §1 didn't run. (Static shared skills are the demonstrated mechanism; a learned fleet memory isn't claimed.)

[[[SVG-CUSTODIAN]]]

## S4.5 The core, on the live platform

Two things ran on the live §3 platform, both working mechanism rather than measured number. Custody: one custodian holds and rotates every per-tenant credential while a credential-free fleet submits specs and gets pass/fail. Orchestration: model routing, cross-vendor review, and skill injection driven natively by a single Omnigent agent.

The capstone ran in two stages: a deterministic wrapper first (the orchestrator decomposes, a script drives routing, custody, review, repair), then the same build native and autonomous (one agent, one custodian tool, the whole loop). From one brief it built end-to-end medallions (bronze to silver to gold) for **three isolated tenants**:

- **Skill:** every worker authored against the one governed `pyspark-sdp` skill.
- **Route:** authoring split across vendors (local Qwen, Claude Opus, OpenAI), re-routing live when one vendor's harness failed to start.
- **Review:** a different-vendor reviewer flagged silent defects the authors missed.
- **Submit through the custodian:** no agent held a credential; per-tenant policy enforced at submit time, including a value-conservation policy that correctly rejected a non-conserving first draft before repair.
- **Repair:** the concrete rejection error fed back, the fleet repaired, escalating to a stronger model until it passed.
- **Isolation held:** every cross-tenant read denied — §3's isolation held under the fleet.

This demonstrates that the mechanism runs, not a numbers claim. Runnable at `deploy/omnigent/sdp-capstone/` (`paper/notes/proof_2026-07-12_sp4_capstone.log`). The paper's own production used the same shape: one orchestrator fanned `claude_code`, `codex`, and `pi` sub-agents, adversarial verifiers tried to refute each finding, and only the survivors got through.

[[[SVG-CAPSTONE-FLEET]]]

**Contained deployment.** For a client, the same layer deploys inside their own cluster: Omnigent server, custodian, and credential-free agent fleet as pods on their EKS over the §3 platform, one IdP governing both (`deploy/kubernetes/`).

[[[SVG-CONTAINED]]]

**Frontier.** Scale-out (many tenants, node autoscaling), credential rotation under live long-running jobs, and a learned fleet memory.

## S4.6 The fleet numbers (separate study, SP4.2)

S4.1 and S4.2's numbers are a separate pre-registered experiment, its own paper, not part of this run and not retrofitted. Design, for completeness: routed vs single-model fleets on dollars-per-correct-pipeline; cross-vendor vs same-vendor defect catch-rate on a seeded corpus; §1's frozen, blind, provenance-tracked instrument at fleet scale. No claim in this paper depends on it.

---

## Appendix S2-A, Reference Architecture: The Control Boundary
*Executable spec for implementing agents. This is the SSOT §2 is measured against; build work lives in `SECTION2_eks_connect_demo_checklist.md`. If a component, step, or claim can't trace to an invariant below, it's drift. Cites are file:line on `origin/dev`.*

> **North star.** The agent proposes inert desired state. A governed control plane, on a separate host, validates and executes. The agent never holds a live session, credentials, or data. The dev loop (propose → dry-run gate → reconcile/execute) is that boundary. Declarative makes it expressible; Spark Connect enforces it.

**The invariant.** The boundary holds iff all of these are true, each a checkable predicate:
- **I1: Authoring ⊥ Execution.** The agent emits only an inert artifact (declarative spec + transform code); it never runs data operations itself.
- **I2: No credentials in the agent.** No mTLS cert/PSK/principal, no warehouse creds, no live `SparkSession`.
- **I3: Host separation.** The process that executes data work runs in a different host/trust zone than the agent.
- **I4: Gate before data.** Structural validation runs and can reject before any data is processed.
- **I5: Governed reconciliation.** A controller the agent doesn't control performs the submit/execute step.
- **I6: Inertness in transit.** What crosses the boundary is a plan/spec, never arbitrary code on a live engine.

A demonstration that violates any I-rule is a simulation of the boundary, not the boundary.

**Trust zones.** Zone U: untrusted authoring — the agent and its workspace, no creds, no session. Zone C: governed control plane — the reconciler/controller, holds creds, runs the Connect client, drives propose→gate→execute→grade; the agent can't run code here. Zone D: data plane (remote EKS) — Connect service (client-mode driver pod) + executor pods + catalog + warehouse, the only place data is touched. Boundary U│C hands the inert artifact (enforces I1); boundary C│D submits plans over authenticated mTLS/PSK Connect (enforces I2/I3/I6).

**Components (role · zone · trust).**

| Component | Zone | Role | Holds creds? |
|---|---|---|---|
| Agent (LLM) | U | proposes SDP spec + transforms as text | **No** |
| Per-cell workspace | U | inert `spark-pipeline.yml` + transform code | No |
| Reconciler / controller | C | runs SDP CLI client; drives gate + execute; blind-grades | **Yes** |
| Dry-run gate | C→D | structural validation before data | via controller |
| Connect channel | C│D | mTLS + bearer PSK + `x-connect-principal` via Envoy | yes (controller-held) |
| Connect server (driver) | D | client-mode driver pod; builds/runs graph | cluster identity |
| Executor pods | D | do the data work | cluster (IRSA) |
| Catalog | D | Iceberg JDBC / HMS | cluster |
| Warehouse (S3) | D | `s3a://…` via IRSA | cluster |
| Blind oracle | C | grades output without seeing paradigm | n/a |

**End-to-end flow, each arrow tagged with its invariant.** Propose (agent emits spec+code, zone U) → *I1*. Materialize (inert artifact written, handed across U│C) → *I1, I6*. Dry-run gate (controller submits a structural dry-run to D; no data touched; defects returned as feedback) → *I4*. Reconcile/execute (controller in zone C ships DefineOutput/DefineFlow/StartRun plans over the authenticated Connect channel) → *I5, I6, I2*. Execute in D (driver + executor pods run against the warehouse/catalog) → *I3*. Telemetry back, blind grade → governance closure.

**The authenticated submission path, the crux.** The controller runs the stock SDP CLI as a Connect client: it imports the agent's transform Python, builds the graph, and sends protobuf plans over gRPC, so the server needs no raw files. Channel: `sc://<NLB>:15009/` + bearer PSK + `x-connect-principal`, terminated by Envoy mTLS. The controller — not a side tunnel — must hold and present this identity (I2).

**Known leaks, named not hidden.**
- **R1: agent code executes in Zone C during plan construction.** The SDP client `exec_module`s the agent's transform Python to build the plan. No data/creds are exposed at that instant, but it is agent-authored code running in the governed zone. Reference stance: acceptable as plan construction only if sandboxed/AST-checked; it's the subtlest part of the boundary.
- **R2: controller co-located with agent.** Today the reconciler runs as a subprocess on the agent/harness host (zones U and C collapsed). The reference requires the split.
- **R3: mTLS via socat tunnel.** The prior remote runs terminated mTLS in a local `socat` tunnel, not the client, which parks the creds in the tunnel host — violates the spirit of I2. The reference requires the controller to present identity natively.

**Working backwards: dependency-ordered layers.** Each layer depends on the one below; claiming a layer while a lower invariant is unproven is drift.

| Layer | Claim it licenses | Enforces | Depends on |
|---|---|---|---|
| **L0 Substrate** | EKS Connect server + Envoy + catalog + S3 reachable | — | — |
| **L1 Authenticated channel** | controller → Connect via native mTLS/PSK, no tunnel | I2 (C│D) | L0 |
| **L2 Off-host execution** | a trivial plan runs in D, driver + executors in pods | I3 | L1 |
| **L3 SDP submission green** | agent-authored spec from C completes + grades green in D | I1, I6 | L2 |
| **L4 Gate before data** | dry-run rejects structural defects pre-execution | I4 | L2 |
| **L5 Governance split** | reconciler in C, agent in U, agent holds no creds | I5, I2, (R2) | L3, L4 |
| **L6 Negative control** | imperative cannot traverse C│D | thesis support | L1 |

**Reverse-engineering map (reference → as-built).**

| Ref element | Target | As-built | Evidence / delta |
|---|---|---|---|
| Inert artifact | I1, I6 | **IMPLEMENTED** | `runner.py:369-405`; spec + code only |
| Plan-not-files submission | I6 | **IMPLEMENTED** (PySpark) | `cli.py:221-263` |
| L0 substrate | — | **APPLIED** via terraform + CI (2026-07-09), S3-backed state | `deploy/eks/terraform` |
| L1 native mTLS/PSK | I2 | **PARTIAL**: socat tunnel, native client unproven (R3) | `study.config.live.json:2` |
| L2 off-host execution | I3 | runs off-host; driver + executors in pods, tables materialize | `study/repro/h3_eks/` |
| L3 SDP green remote | I1, I6 | Arm B completes + grades green remotely (2026-07-06), submitted session-less | `study/repro/h3_eks/` |
| L4 gate before data | I4 | **IMPLEMENTED locally**; not re-proven remote | `sdp_dryrun.py:462-484` |
| L5 governance split | I5, I2, R2 | **GAP**: reconciler co-located on agent host | `live.py:675-689` |
| L6 imperative-can't-cross | thesis | **CORROBORATED, not captured** | `DEVIATIONS.md:516-522` |

Highest honestly claimable: ~**L3** (both arms run the full loop remotely; arm B completes + grades green, 2026-07-06). L5 is the real remaining work; L1 needs native-mTLS de-risking; L6 needs capturing as a clean artifact. Claim only up to the highest proven layer, and state the next gap plainly.

---

## Appendix S3-A, Reference Architecture: The Open Governed Platform
*Executable target for the multi-tenant platform §3 builds toward; build work lives in `SECTION3_platform_build_checklist.md`. As in S2-A: claim only up to the highest proven layer; anything above it is a build task, not a result.*

**G: Invariants (what the platform must satisfy).**
- **G1 identity at the ingress:** every request arrives with a cryptographically pinned principal; none is trusted from a claim.
- **G2 least-privilege custody:** the agent never holds credentials; per-tenant tokens are injected server-side and vended per request.
- **G3 execution isolation:** each tenant's work runs on its own executor pods; no shared JVM in the proven topology.
- **G4 catalog authorization:** every catalog operation, including the vend, is authorized per principal.
- **G5 storage scoping:** every vended credential is downscoped to the tenant's exact prefix.
- **G6 GitOps:** platform changes land via tested PRs, reconciled by a controller, not by an agent session.

**P: Build/claim layers (bottom-up; the highest proven layer is the honest claim ceiling).**

| Layer | Requirement | Status |
|---|---|---|
| **P0 substrate** | EKS cluster applied via terraform, CI/OIDC-gated | **APPLIED (2026-07-09)** |
| **P1 governed ingress** | single mTLS door; no raw Connect exposure | **runs today** |
| **P2 authoring boundary** | agent-as-PR-author + CI dry-run gate + reconcile | runs locally today |
| **P4 integration testing** | structural dry-run against the real catalog | runs today; data-quality tests next |
| **P5 tenant isolation** | credential scoping + per-principal authz + per-tenant execution | **runs today, link by link** |
| **P6 multi-tenant scale** | multiple Connect servers + node autoscaling | per-tenant servers + routing run today (2 proof tenants; a third for the §4 capstone); N-tenant scale + autoscaling next |

P5 detail, since it's the load: storage scoping (Lakekeeper vends, replay cross-tenant `AccessDenied` both directions, 12-task per-write delta on a dedicated executor pod, CloudTrail: FileIO via the vend, fleet IRSA 0 data calls, ablation confirms the vend is load-bearing); per-principal catalog authz (tenant-A denied for tenant-B across warehouse resolution, namespace, and the direct vend path — `loadTable` delegation: tenant-B `200`+creds, tenant-A `404` — grants toggle, `proof_2026-07-10_perprincipal_authz.log`); token custody + execution isolation (two per-tenant servers, server-injected tokens, tenant-A session refused on tenant-B, disjoint executor pods, `proof_2026-07-10_multiserver.log`); ingress routing by identity (Envoy by cert URI-SAN, un-granted `403`, no-cert refused, `proof_2026-07-10_ingress_routing.log`).

**Highest honestly claimable ≈ the full per-tenant isolation path, link by link on EKS (2026-07-09/10).** Tenant A is routed to tenant-A's server, handed tenant-A's token, run on tenant-A's executors, authorized at the catalog only for tenant-A, prefix-scoped at storage. What remains: multi-tenant scale (many tenants + node autoscaling), a single request composing all five links, and unifying the storage-scoping forensic onto the authorization-enabled catalog.

**R: Reverse-engineering map (reference → as-built → SALVAGE / GAP).**

| Component | Target | As-built | SALVAGE (keep) | GAP (build) |
|---|---|---|---|---|
| GitOps loop | agent→PR→CI gate→reconcile to prod | runs locally | PR authoring, dry-run + reconcile workflows, unit tests | wire to EKS Connect; capture a real agent PR |
| Connect ingress | one governed mTLS endpoint + per-principal routing | built; Envoy routes by cert-SAN to per-tenant servers | Envoy mTLS, principal interceptor, gateway, deployment, image | capture deploy artifacts |
| Elastic execution | driver + dyn executors, autoscaling | runs at small scale | dyn-alloc config, pod template, node group, image | prove 0→10→0; add Karpenter/autoscaler |
| Catalog authz | per-tenant grants | runs via Lakekeeper + OpenFGA + OIDC | governed Iceberg-REST catalog, per-tenant vending, OIDC identities | OSS HMS/Iceberg still can't enforce, hence the governed catalog |
| Tenant exec isolation | per-tenant Connect / pools | runs today: per-tenant servers, gateway routing, disjoint pods | per-tenant servers, routing gateway, token injection | scale to N tenants + node autoscaling |
| Integration testing | structural + data-quality gate | structural only | the CI dry-run gate | data-quality / expectation tests in CI |
| Substrate (IaC) | reproducible cluster | terraform-applied + CI/OIDC-gated, live (2026-07-09) | terraform stack, k8s manifests, HMS, image | keep capturing run evidence per deploy |

**Read R top-to-bottom to build:** P0→P4 is mostly salvage + wiring; P5/P6 is genuine new construction (a governed catalog + multi-server Connect).
