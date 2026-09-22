# Section 1 — reproduction runbook

Reproduce the Section-1 (imperative-vs-SDP) results, and **re-run on a new Spark version** to see
what changes. All scripts here are self-locating (they find the study dir relative to themselves),
so the whole `repro/` tree can be moved or the repo re-cloned without editing paths.

## 0. Environment (record this every run)
```bash
python3 -c "import pyspark; print(pyspark.__version__)"   # baseline run: 4.1.0.dev4
git describe --tags                                        # instrument: instrument-v3.2-frozen
```
The instrument (harness + corpus + skills) is frozen at tag **`instrument-v3.2-frozen`**. To reproduce
the exact instrument, `git checkout instrument-v3.2-frozen` first. The **Spark version is NOT pinned by
the instrument** — it's whatever `pyspark` is installed — which is the point: swap Spark, keep the
instrument, diff the numbers.

## 1. Primary powered run (528 cells = 22 tasks × 12 seeds × {A,B})
Local backend: imperative (A) runs on classic local Spark, SDP (B) on a local Connect server.
```bash
cd experiments/safe_agent_study
python3 -m harness.runner --backend local --only-arms A,B --max-seeds 12 \
  --local-connect-port 15040 --local-ui-port 4080 --per-cell-timeout 1800 \
  --out results/results.powered.jsonl --work-dir .work.powered
```
Notes: a **circuit breaker** aborts the run if >1 arm/bin harness-fault (e.g. a transient API blip);
that's intended — fix the cause and backfill the missing cells (§2). Long agent-authoring loops can hit
the 1800s per-cell cap (`harness_error`/`PER_CELL_TIMEOUT`); those are re-run in the backfill too.

## 2. Backfill (only if the breaker tripped or cells timed out)
`repro/backfill/` targets *exactly* the affected cells via subset seed-files (the runner opens `--out`
in truncate mode, so each invocation writes its own file — do **not** share one `--out`):
```bash
bash repro/backfill/run_backfill.sh            # edit the task/seed sets inside for your gaps
python3 repro/backfill/merge_and_analyze.py    # merges primary + backfill (backfill wins) -> *.final.jsonl + report
```

## 3. Analyze → headline
```bash
SPARK_HOME=$(python3 -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))') \
python3 analysis/analyze.py results/results.powered.final.jsonl --tasks config/TASKS.lock.json \
  --assume-backend local --md-out HEADLINE.md --json-out REPORT.json
```

## 4. D7 attribution test — **the Spark-version-sensitive one** ⭐
Tests whether SDP's timezone (D7) loss is a paradigm limit or a skill gap. Augments the frozen
`pyspark-sdp` skill with a UTC column idiom, re-runs arm B on the 3 D7-shipping tasks, **restores the
frozen skill**, and compares D7 ships:
```bash
bash repro/tzfix_d7_test/run_tzfix_d7.sh       # augment -> run B on p8/stream_join/p14 -> restore -> compare
```
**On a new Spark (esp. 4.2):** if the framework adds a *symmetric, declarative* way to pin
`spark.sql.session.timeZone` (a `configuration:` block in `spark-pipeline.yml` or a per-view arg), then
the **baseline** frozen-B D7 may already be ~0 *without* the skill fix — i.e. the framework gap is
closed. That is exactly the result to look for. (Baseline 4.1.0.dev4: frozen-B D7 = 7 → tzfix-B D7 = 0.)

## 5. Corpus v3.1 supplemental (optional, non-CDC High task)
`repro/corpus_v31/` has the `new_lineitem_reconcile` task JSON, its DEVIATIONS entry, and
`stage_and_validate.py` (validates a staged 23-task lock against `harness.complexity` + `test_corpus`).
Note: on 4.1 this task **saturates on D6** (both arms 100% silent) — it contributes to structural-catch
but not the silent-defect endpoint. See DEVIATIONS.D-CORPUS-V31.md.

---

## Baselines — Spark **4.1.0.dev4**, `instrument-v3.2-frozen` (diff a new-Spark run against these)

| metric | A (imperative) | B (SDP) | note |
|---|---|---|---|
| **H1 silent-defect rate** | 0.277 (73/264) | 0.326 (86/264) | OR 1.97, p=0.0033 — *skill-induced, see D7* |
| **H1.1 structural-catch (gate)** | 0 intercepts | **79** (353 iter-level) | clean paradigm property |
| — D6 dedup (silent ships) | 38 | 39 | **wash** — paradigm-neutral |
| — D7 timezone (silent ships) | 0 | **7** | **skill gap** (see below) |
| — D8 row-drop (silent ships) | 51 | 57 | mostly wash + missing epoch branch |
| **D7 attribution** (test §4) | — | **7 → 0** | with UTC skill idiom → parity |
| **H4 conciseness** LOC | 134.0 | 67.9 | B ~49% fewer |
| H4 conciseness AST | 1105.5 | 614.9 | B ~44% smaller |
| **H2 tokens** (median total) | 11,524 | 26,480 | B ≈ 2.3× (SDP iterates more) |
| **H5 correct-completion** | 68.9% (182/264) | 65.2% (172/264) | gap tracks the skill-induced silent gap |
| completion rate | 96.6% | 97.7% | — |

Powered: N=264 (task,seed) cells ≥ 260 required; 0 instrument-fault rows in the final set.

## What to watch when Spark changes (esp. 4.1 → 4.2)
1. **Native AutoCDC (SCD-1/2).** 4.1.0.dev4 has **no** `create_auto_cdc_flow`/`apply_changes` — the 7
   CDC/SCD/MERGE tasks are hand-rolled for both arms. 4.2 native AutoCDC could change those tasks
   substantially. (7 tasks: p2_cdc, p10_scd2, p13_cdc_windowed, new_merge_upsert, new_scd2_as_of_join,
   new_cdc_tombstone, HC1_fx_trade_ledger.)
2. **Session-timeZone in SDP (the D7 framework gap).** Re-run §4. If 4.2 lets SDP pin session tz
   declaratively, baseline-B D7 should drop toward 0 on its own.
3. **`spark.ui.retainedStages`** (Phase-2b compute only): raise it above total stage count or the
   stage-diff executor-seconds undercount.
4. **UC-OSS full-refresh truncate** (Phase-2b, real catalog only): a UC-OSS-specific hazard; the local
   `in-memory` catalog used here is unaffected.
