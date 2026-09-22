# Five demos: the paper, shown from a developer's chair

Purpose: show the *application* of the paper, not the argument. Each demo puts a viewer in front of
a terminal and lets them watch what it is actually like to build Spark pipelines this way. Each one
makes exactly one claim from the paper visible, and each runs on infrastructure that exists today
(this repo, the lakehouse-stack docker services, and the live `ssa-spark-eks` cluster).

The arc, in order, is laptop to governed platform:

| # | Demo | Paper claim shown | Substrate | Time |
|---|------|-------------------|-----------|------|
| 1 | The gate fires before any data | §1.4.2, §2.2: structural defects die at dry-run, at zero compute | laptop (local Connect) | 5-7 min |
| 2 | Watch a real agent converge | §2.6-2.7: the propose → gate → reconcile loop, run by a real agent | laptop + API key | ~10 min |
| 3 | Same task, two paradigms | §1.4.1-1.4.3: half the code, gate-catch vs runtime failure, the powered numbers | laptop + API key | ~15 min |
| 4 | The agent's only artifact is a PR | §3.1: session-free authoring, CI as the gate, controller-owned reconcile | laptop + GitHub | ~12 min |
| 5 | One endpoint flip, then a governed no | §2.8, §3.3: dev-to-prod portability and five-layer tenant isolation | live EKS (docker fallback) | ~15 min |

Demos 1, 2, and 4 stand alone. For a 20-minute slot: 1 + 2 + the analyze.py closer from 3.
For a platform/security audience: 1 + 4 + 5.

---

## Shared prep (once, ~10 minutes)

All demos assume this repo (`<repo>`, e.g. `~/safe-spark-agents-paper`) and the host toolchain already present:
PySpark 4.1.0.dev4 and the `spark-pipelines` CLI on PATH, Java 17 installed, `ANTHROPIC_API_KEY` set.

```bash
# 1. Java: JAVA_HOME is not set by default on this host
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))

# 2. Local Spark Connect server (demos 1, 2, 4). Runner-local, port 15055.
cd <repo>/study/gitops_demo && ./local_spark_connect.sh start

# 3. EKS (demo 5 only): confirm the cluster and tenant servers are up
kubectl config current-context        # arn:...:cluster/ssa-spark-eks
kubectl -n spark get pods | grep -E 'gateway|tenant'
```

Gotchas to know before going on stage:

- The grading oracle (`defect_battery/`) is vendored at the repo root; demos 2 and 3 need it and
  the grader hard-refuses without it, by design (one oracle source).
- The first Spark launch of the day is slow (JVM + JIT). Warm everything before the audience arrives.

---

## Demo 1: The gate fires before any data

**Claim shown.** SDP's structural dry-run rejects a broken pipeline graph before a single row is
read, at effectively zero compute; imperative discovers the same defect only by running, after it
has already paid for every stage upstream of the failure (§1.4.2: 79 caught at gate vs 0; §1.4.3 N2:
wasted compute finite vs ≈ 0).

**Shape.** No agent, no slides. You hand-author a three-table pipeline live, sabotage it, and let
the framework do the talking. The whole demo is the stock OSS `spark-pipelines` CLI from Apache
Spark 4.1, which is worth saying out loud: nothing here is Databricks-proprietary.

**Setup.**

```bash
mkdir -p /tmp/demo1/transformations && cd /tmp/demo1
python3 <repo>/generators/gen_messy_orders.py --seed 42 --N 50000 > orders.ndjson
```

`spark-pipeline.yml`:

```yaml
name: demo_gate
storage: file:///tmp/demo1/storage
catalog: spark_catalog
database: demo_gate
libraries:
  - glob:
      include: transformations/**/*.py
```

`transformations/pipeline.py`, a bronze → silver → gold graph:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view
def bronze_orders():
    return spark.read.json("file:///tmp/demo1/orders.ndjson")

@dp.materialized_view
def silver_orders():
    return (spark.read.table("bronze_orders")
            .where(F.col("order_id").isNotNull())
            .dropDuplicates(["order_id"]))

@dp.materialized_view
def gold_daily_revenue():
    return (spark.read.table("silver_orders")
            .groupBy(F.to_date("order_ts").alias("day"))
            .agg(F.sum("amount").alias("revenue")))
```

**Script.**

1. *Baseline green.* `SPARK_REMOTE=sc://localhost:15055 spark-pipelines dry-run` then
   `spark-pipelines run`. The run materializes all three tables: `Run is COMPLETED`.
2. *Sabotage.* Edit `gold_daily_revenue` to read `silver_orderz` (or delete `silver_orders`
   entirely). Say what you did; this is a stand-in for what an agent hallucinates.
3. *The gate.* `spark-pipelines dry-run` again. It fails in seconds with
   `[TABLE_OR_VIEW_NOT_FOUND] ... SQLSTATE: 42P01`, and the thing to narrate is what did NOT
   happen: no data read, no executor started, no table touched. The whole graph was resolved and
   rejected as a graph.
4. *The imperative contrast.* Same logic as one `spark-submit` script
   (bronze write, silver write, then the typo'd gold). Run it: bronze and silver actually compute
   and write, real work, real seconds, and only then does gold blow up. The viewer watches the
   money burn before the error. This is §1.4.3's N2 asymmetry lived in 30 seconds.
5. *Fix and converge.* Repair the name, dry-run green, run, done.

**The money moment.** Step 3 vs step 4 back to back: identical defect, one dies inert in two
seconds, one dies after paying for two stages of compute.

**Variant.** Point bronze at the running lakehouse-stack Kafka (`orders` topic, port 9092) with a
`@dp.table` streaming table if you want the streaming flavor; the gate behaves identically.

---

## Demo 2: Watch a real agent converge

**Claim shown.** The agent-native loop from §2 is real and observable: a real model proposes
desired state, the gate bounces it with structured feedback, the agent revises, converges, and is
blind-graded, all without ever holding a session (§2.6-2.7).

**Shape.** One live (task, seed) cell from the actual study harness, arm B, the same instrument the
528-run powered result came from. Nothing is mocked; the audience watches the loop the paper
measured.

**Script.**

1. Show the task brief first so the audience knows what the agent was asked
   (`orders_silver_gold` in `study/config/TASKS.lock.json`: Kafka-shaped messy orders to silver to a gold
   daily revenue rollup).
2. Launch the cell (this is the SM6.8-verified invocation, narrowed to one cell):

```bash
cd <repo>/study
python3 harness/runner.py \
  --backend local --config config/study.config.json --arms-dir arms \
  --tasks config/TASKS.lock.json --seeds config/SEEDS.lock.json \
  --only-tasks orders_silver_gold --only-arms B --max-seeds 1 \
  --out /tmp/demo2/results.jsonl --work-dir /tmp/demo2/work \
  --per-cell-timeout 1800
```

3. Narrate the loop as it runs: propose → dry-run gate → feedback → revise. When the gate
   intercepts an early iteration, that is the demo working, not failing; say so.
4. When the cell goes green, open the receipts:
   - `/tmp/demo2/work/**/transcript.json`: every iteration's messages, gate verdicts, token counts.
   - the `ResultRow` in `/tmp/demo2/results.jsonl`: `dry_run` intercepts, `detection_stage`,
     `final_program_loc`, tokens, git provenance.
   - the final `transformations/pipeline.py` the agent wrote: decorated transforms only, no
     session, no `.write`, no credentials. Point at what is *absent*.

**The money moment.** The transcript showing iteration 1 rejected at the gate with the same
`SQLSTATE` the viewer saw in demo 1, and the final program green a few iterations later. The loop
closed before any data every time it needed to.

**Fallbacks.** Live model latency makes this 5-10 minutes of runtime; either run it while talking
through demo 1's recap, or pre-run one cell and walk the committed transcript instead (identical
artifacts, zero risk). `--backend replay` exercises the plumbing with no key and no Spark if you
ever need a dry rehearsal.

---

## Demo 3: Same task, two paradigms, split screen

**Claim shown.** Section 1, experienced instead of graphed: the imperative agent writes twice the
code and learns about mistakes only at runtime; the SDP agent gets bounced at the gate, converges,
and ships half the code at more tokens (§1.4.1-1.4.3).

**Shape.** A tmux split. Left pane arm A, right pane arm B, same task, same seed, same model.
This is literally one paired cell of the study.

**Script.**

1. Fire both panes (same command as demo 2, `--only-arms A` on the left, `--only-arms B` on the
   right, distinct `--out`/`--work-dir`).
2. While they run, narrate the structural difference: left writes a program that owns a
   `SparkSession` and must be executed to be tested; right writes inert decorated transforms that
   get dry-run as a graph.
3. When both finish, compare the artifacts on screen:

```bash
wc -l /tmp/demo3/a/work/**/final_program.py /tmp/demo3/b/work/**/transformations/pipeline.py
python3 - <<'EOF'
import json
for arm in ("a", "b"):
    row = json.loads(open(f"/tmp/demo3/{arm}/results.jsonl").readline())
    print(arm.upper(), "| iters:", row["iterations"],
          "| gate intercepts:", row.get("dry_run_intercepts"),
          "| tokens:", row.get("total_tokens"), "| loc:", row.get("final_program_loc"))
EOF
```

4. Close by recomputing the paper's headline table from the committed 528-row powered run, live,
   no Spark and no model needed:

```bash
cd <repo>/study
python3 analysis/analyze.py results/results.powered.AB.n12.final.jsonl \
  --tasks config/TASKS.lock.json --assume-backend local
```

**The money moment.** "What you just watched in two panes is one cell. Here are all 528," and the
table prints: 79 vs 0 at the gate, ~49% fewer lines, 2.3x tokens, silent-defect parity once the
skill teaches the UTC idiom.

**Optional extension.** The D7 story for a skeptical audience:
`study/repro/tzfix_d7_test/run_tzfix_d7.sh` reruns the skill-swap that drives the timezone class
from 7 to 0 (needs the `defect_battery` symlink; budget extra time).

---

## Demo 4: The agent's only artifact is a PR

**Claim shown.** §3.1: the authoring boundary operationalized as GitOps. The agent can write files
and open a PR, and can do nothing else; CI runs the real dry-run gate against the real catalog;
a controller, never the agent, reconciles on merge.

**Shape.** `study/gitops_demo/`, self-contained and already verified end to end locally. The
GitHub Actions workflows (`.github/workflows/gitops-sdp-dry-run.yml`, `gitops-sdp-reconcile-local.yml`)
are in this repo, so a real PR triggers the real gate.

**Script.**

1. *Prove the handcuffs first.* Before showing what the author does, show what it cannot do:

```bash
cd <repo>/study/gitops_demo
SPARK_REMOTE=sc://localhost:15055 python3 agent_pr_author.py --task tasks/orders_silver_gold.json \
  --pipeline-slug should-refuse   # refuses: exits nonzero because SPARK_REMOTE is set
python3 -m pytest tests/ -q       # boundary tests: no pyspark import, git/gh-only subprocess allowlist
```

2. *Author.* Unset `SPARK_REMOTE`, run the author for real:

```bash
python3 agent_pr_author.py --task tasks/orders_silver_gold.json --pipeline-slug demo-$(date +%m%d)
```

   The Arm-B brain writes the SDP artifact, and the process's entire footprint is
   `git checkout -b` / `add` / `commit` / `push` / `gh pr create`. Show the PR in the browser:
   a spec + transforms, inert text, reviewable like any code change.
3. *The gate as CI.* The PR triggers the dry-run workflow: it stands up Connect, ensures the
   schema, dry-runs the changed spec. For the failure beat, push a second PR whose spec reads a
   missing upstream: the check goes red with `TABLE_OR_VIEW_NOT_FOUND ... SQLSTATE 42P01`,
   before merge, before any data.
4. *Merge, reconcile.* Merge the good PR; the reconcile workflow runs `spark-pipelines run` as the
   controller and materializes the tables. The agent that authored it never held `SPARK_REMOTE`,
   a session, or a credential.

**The money moment.** The red check on the broken PR. Every developer in the room knows exactly
what that means: agent-written pipelines just got the same discipline as human-written code, with
a structural integration gate no linter can fake.

**Fallback (no GitHub access in the room).** Run the same three stages by hand:
`ensure_schema.py` → `python3 ../harness/sdp_dryrun.py --spec <dir>/spark-pipeline.yml` →
`SPARK_REMOTE=sc://localhost:15055 python3 reconcile.py`, using `--no-pr` on the author.

---

## Demo 5: One endpoint flip, then a governed no

**Claim shown.** Two claims, deliberately paired. First §2.8: dev-to-prod is one environment
variable, because a Connect client ships plans to a URL. Second §3.3: the production end of that
URL is a governed platform where tenant isolation is enforced at five independent layers, live.

**Shape.** Part A is portability; part B is the isolation walk. Runs against the live
`ssa-spark-eks` cluster, where the full §3.3 stack is deployed right now: gateway Envoy,
per-tenant Connect servers (a, b, c), Lakekeeper + OpenFGA, per-tenant executor pods.

**Part A: the flip.**

1. Locally: `SPARK_REMOTE=sc://localhost:15055 python3 connect/client.py <table>` (or rerun demo
   1's pipeline). Point at the line it prints: `connected via sc://... (all execution is remote)`.
2. Change nothing but the URL: set `SPARK_REMOTE` to the EKS Connect endpoint (through the mTLS
   tunnel per `deploy/eks/RUNBOOK.md`; the paper is explicit that native client mTLS is the L1 gap,
   say so on stage, it reads as honesty, not weakness).
3. In a second pane: `kubectl -n spark get pods -w`. The viewer watches executor pods appear as
   the same client code runs, and the tables land in the governed catalog on S3. Same spec, same
   gate, different endpoint: that is the whole dev-to-prod story.

**Part B: the governed no.** Walk the layers outside-in, each one a live denial:

1. *Ingress.* A tenant-a client certificate through the gateway reaches only
   `spark-connect-tenant-a`; an un-granted principal gets 403; no cert is refused at TLS.
2. *Catalog.* With the `lakekeeper-authz` port-forward up, run
   `deploy/eks/lakekeeper/authz/authz_proof.sh`: tenant-a's identity gets 404 for tenant-b's
   namespaces (existence hidden), tenant-b gets 200, and the grant toggle flips 404 to 200 to
   prove the deny is authorization, not absence.
3. *Storage.* Replay tenant-a's vended, prefix-scoped credential against tenant-b's prefix:
   `AccessDenied`, both directions.
4. Show the audience the pods while this happens: two tenants, two drivers, disjoint executor
   pods, no shared JVM.

Keep `paper/notes/proof_2026-07-10_*.log` open in a spare pane as the reference transcript of what
each step should print; if a live step misbehaves, the committed proof log is the honest fallback.

**The money moment.** The same terminal that got a green `Run is COMPLETED` as tenant-a gets a 404
the instant it asks about tenant-b, and the narration writes itself: the platform trusts the agent
with nothing, and it still got its pipeline built.

**No-cloud fallback.** `deploy/eks/lakekeeper/spike/run.sh` stands up the credential-vending
isolation story entirely in docker-compose (Spark 4.1.2 + Lakekeeper + MinIO, 14/14 checks,
cross-tenant `AccessDenied` included) on a laptop. Use it when there is no cluster or no network.

---

## Rehearsal checklist

- [ ] `JAVA_HOME` exported; `spark-pipelines --help` works
- [ ] `defect_battery` symlink in place (demos 2, 3)
- [ ] local Connect up on 15055; demo 1 baseline green end to end
- [ ] one pre-run cell archived for demo 2's fallback
- [ ] `gh auth status` clean; Actions enabled on the repo (demo 4)
- [ ] kubectl context on `ssa-spark-eks`; gateway + tenant pods Running; port-forward tested (demo 5)
- [ ] proof logs open in a spare pane (demo 5)
- [ ] first-run JVM warmup done within the hour before presenting
