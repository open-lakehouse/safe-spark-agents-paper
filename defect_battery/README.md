# Defect battery (EXPERIMENT_DESIGN.md E3)

Tests the project hypothesis directly: Spark Declarative Pipelines (SDP) + the
`spark-pipelines dry-run` gate catch **structural** defects (missing column,
broken DAG, immutable config) at analysis time — exit 1, a specific error class,
zero data read — but do **not** catch **semantic / state** defects (wrong-type
parse, unwatermarked dedup, non-deterministic dedup, timezone bug, absent
quarantine), which pass `dry-run` COMPLETED and corrupt the output silently. The
robustness gain is real but bounded.

The battery is run end-to-end by [`run_battery.sh`](run_battery.sh). The actual
measured outcomes are in [`results.jsonl`](results.jsonl) (machine-readable) and
[`E3_RESULTS.md`](E3_RESULTS.md) (human-readable, regenerated each run).

## What's here

```
variants/<Dn>_<name>/        one self-contained SDP project per defect:
    spark-pipeline.yml         spec (name, catalog, storage, libraries[, configuration])
    transformations/<dn>.py    the flow, with exactly one injected defect
    conf/spark-defaults.conf   connector jars (spark.jars, __REPO__-templated)
plain_pyspark/               classic-Spark (non-SDP) contrast arms for D1, D2
quantify.py                  measures the silent corruption of D2/D6/D7/D8 over the dataset
run_battery.sh               the harness (below)
results.jsonl / E3_RESULTS.md  outputs
```

Note: there is **no** per-variant `sdp/` + `imperative/` split. Each variant *is*
the SDP arm; the only standalone classic-Spark arms are `plain_pyspark/d1_plain.py`
and `plain_pyspark/d2_plain.py`, kept as an explicit contrast (D1 in particular:
SDP fails the whole graph at dry-run, whereas classic PySpark only raises when
that one statement is touched).

## Scoring rubric (per defect × approach)

`stage` is one of: `analysis` (caught before any data is read), `completed`
(dry-run accepts it, exit 0), `silent-wrong` (materializes a wrong/under-counted
output), `resource-failure` (OOM / unbounded state, cluster-only). The
`results.jsonl` schema is:

```json
{"defect","approach","stage","exit_code","error_class","wall_s","rows_affected","verdict"}
```

`approach` is `sdp_dry_run` (the real SDP analysis gate) or `batch_materialize`
(a classic batch that quantifies the corruption the gate misses).

## The defects

| # | Class | Injection | Hypothesis: SDP dry-run |
|---|---|---|---|
| D1 | missing/renamed column | reference a column not in the schema | analysis (UNRESOLVED_COLUMN, exit 1) |
| D2 | wrong type in parse schema | epoch-ms field typed TIMESTAMP in from_json | completed → silent-wrong |
| D3 | unwatermarked streaming dedup | `dropDuplicates` on a stream, no watermark | completed (state failure is runtime/cluster) |
| D4 | broken DAG / missing upstream | read a table no flow produces | analysis (TABLE_OR_VIEW_NOT_FOUND, exit 1) |
| D5 | invalid/immutable config | set a static conf in the spec `configuration:` block | analysis (CANNOT_MODIFY_CONFIG, exit 1) |
| D6 | non-deterministic dedup | dedup with no ordering/sequence key | completed → silent-wrong (latent, see results) |
| D7 | timezone/day-bucket bug | derive date in session-local tz | completed → silent-wrong |
| D8 | absent bad-record handling | no quarantine; malformed rows corrupt aggregates | completed → silent-wrong |
| D9 | unbounded state → OOM (cluster) | D3 under a long-running stream at scale | completed (resource-failure cluster-only) |

These are the *hypothesis*. The *measured* result for every row is in
`results.jsonl` / `E3_RESULTS.md` — all nine were actually run.

## How to run

```bash
bash defect_battery/run_battery.sh
cat  defect_battery/results.jsonl
```

The harness:

1. Generates the deterministic seed=42 dataset (`generators/gen_messy_orders.py`,
   5276 messy rows) into a gitignored `.work/`.
2. Stands up one Spark Connect server with the Kafka connector on the launch
   classpath and `spark.sql.artifact.isolation` **off**, then runs the real SDP
   `dry-run` (`pipelines/cli.py dry-run`, the same entrypoint the
   `spark-pipelines` wrapper calls) for each variant via `SPARK_REMOTE`.

   > Why a self-managed server: the stock `spark-pipelines` wrapper enables
   > artifact isolation, which loads `spark.jars` into a classloader the Kafka
   > DataSource ServiceLoader can't see, so Kafka-source variants fail dry-run
   > with `Failed to find data source: kafka` — an environment artifact, not the
   > defect. This runs the same SDP dry-run CLI analysis path (`pipelines/cli.py
   > dry-run`); only the Connect server launch, classpath, and
   > `spark.sql.artifact.isolation` setting differ, to remove the
   > Kafka-connector-visibility confound.

3. Records exit code, error class / SQLSTATE, and wall time per variant.
4. For the silent-wrong semantic defects (D2/D6/D7/D8) runs `quantify.py` over
   the same dataset and records `rows_affected`. The corruption is a property of
   the parse/aggregation over the data, not of the source, so it is measured from
   the generated NDJSON — **no Kafka broker is required** (dry-run reads no data
   either).

Requires a pyspark 4.1 with the `pipelines` module and the connector jars under
`<repo>/connect/jars/`. Verified on `pyspark 4.1.0.dev4`.
