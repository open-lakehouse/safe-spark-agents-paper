"""OUTPUT oracles: grade the agent's MATERIALIZED table, not the input (B1).

The input quantifiers (`quantify.py` / `quantify_ext.py`) measure the defect
*opportunity* on the source data (and reproduce the registered E3 numbers). They
are NOT the live silent-defect oracle: a run is silent iff the COMPLETED output
the agent shipped is still wrong. So for a live run we read the agent's
materialized table back through the same Spark session and compare it to ground
truth derived from the matched-seed input.

Every function here is arm-agnostic: it sees the output table reader, the input
path, and the task's output contract -- never the arm, model, or whether a gate
ran. The runner calls `build_output_profile(...)` on a completed run and feeds
the result to the (blind) grader.

Output contract (per task, in TASKS.lock.json `output_contract`): the table name
and the columns the agent was told to produce, so the oracle can read a known
interface:
    {"table": "...", "revenue_col": "...", "date_col": "...", "key_col": "...",
     "currency_col": "...", "substrate": "orders|payments|cdc"}

Residual semantics (what makes a COMPLETED output silently wrong):
  D8  output revenue total does NOT reconcile to the true total (value dropped).
  D2  output has rows bucketed to an impossible (epoch-misparse) far-future date.
  D7  output per-day buckets disagree with the UTC-correct buckets.
  D6  output's surviving row per key is not the deterministic (seq/time-max) one.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from harness import oracles as oraclesmod

# reconciliation tolerance (USD) -- below this an output total counts as reconciled
RECON_TOL = 1.0


# ---------------------------------------------------------------------------
# ground truth from the matched-seed input (reuses the quantifier schemas)
# ---------------------------------------------------------------------------
def _orders_true_total(spark, input_path: str) -> float:
    """Correct revenue total: every present scalar amount (numeric OR quoted-string)
    PLUS, for v3 line_items rows that carry no scalar amount, the nested revenue
    sum(qty*price). This is what a correct gold table's sum(revenue) must equal; an
    undercount means rows were silently dropped (D8, incl. the nested-array form).
    On a v2 (no-line_items) stream the nested term is 0, so this is unchanged."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, ArrayType, LongType, DoubleType)
    schema = StructType([
        StructField("order_id", StringType()), StructField("merchant_id", StringType()),
        StructField("event_time", StringType()), StructField("amount", StringType()),
        StructField("line_items", ArrayType(StructType([
            StructField("sku", StringType()), StructField("qty", LongType()),
            StructField("price", DoubleType()),
        ]))),
        StructField("category", StringType()),
    ])
    j = spark.read.text(input_path).select(F.from_json("value", schema).alias("s")).select(
        F.col("s.amount").alias("amount"), F.col("s.line_items").alias("line_items"))
    scalar_ok = F.col("amount").rlike(r"^-?[0-9]+(\.[0-9]+)?$")
    scalar_total = j.filter(scalar_ok).agg(
        F.sum(F.col("amount").cast("double")).alias("d")).collect()[0]["d"] or 0.0
    li_rev = F.expr("aggregate(line_items, cast(0.0 as double), (acc, x) -> "
                    "acc + coalesce(x.qty,0) * coalesce(x.price, cast(0.0 as double)))")
    nested_total = j.filter(
        F.col("line_items").isNotNull() & (F.size("line_items") > 0) & ~scalar_ok
    ).agg(F.sum(li_rev).alias("d")).collect()[0]["d"] or 0.0
    return float(scalar_total) + float(nested_total)


def _load_fx():
    """The ONE daily-FX source of truth (generators/fx.py)."""
    import importlib.util
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    # Mirrors runner.py._find_repo_root: generators/ sits at the repo root, which is two
    # levels up in the paper repo and three in the original layout. Walk up.
    root = os.environ.get("STUDY_REPO_ROOT")
    if not root:
        d = here
        for _ in range(6):
            d = os.path.dirname(d)
            if os.path.isdir(os.path.join(d, "generators")) or os.path.isdir(os.path.join(d, "infra")):
                root = d
                break
        else:
            root = os.path.normpath(os.path.join(here, "..", "..", ".."))
    fxpath = os.path.join(root, "generators", "fx.py")
    if not os.path.exists(fxpath):  # pre-reorganization layout compat
        fxpath = os.path.join(root, "infra", "fx.py")
    spec = importlib.util.spec_from_file_location("infra_fx", fxpath)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _payments_true_usd_total(spark, input_path: str) -> float:
    """Correct USD total: every present amount FX-converted to USD at the rate in
    effect on the payment's OWN UTC date (v3 daily FX), foreign rows included,
    excluding null/bad-currency. An undercount means foreign value was silently
    dropped (D8 on payments)."""
    import datetime as dt
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType)
    fxmod = _load_fx()
    dates = [fxmod._FX_EPOCH + dt.timedelta(days=i) for i in range(-1, 10)]
    rate_map = {f"{c}|{d.isoformat()}": r for (c, d), r in fxmod.rate_table(dates).items()}
    schema = StructType([
        StructField("payment_id", StringType()), StructField("account_id", StringType()),
        StructField("event_time", TimestampType()), StructField("currency", StringType()),
        StructField("amount_minor", StringType()), StructField("amount", StringType()),
        StructField("settled", StringType()),
    ])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    rows = spark.read.text(input_path).select(F.from_json("value", schema).alias("s")).select(
        F.col("s.currency").alias("currency"), F.col("s.amount").alias("amount"),
        F.to_date(F.col("s.event_time")).alias("utc_date"))
    fx_map = F.create_map([x for k, v in rate_map.items() for x in (F.lit(k), F.lit(v))])
    key = F.concat_ws("|", F.col("currency"), F.date_format("utc_date", "yyyy-MM-dd"))
    total = rows.filter(
        F.col("currency").isin(list(fxmod.BASE_FX)) & F.col("amount").rlike(r"^-?[0-9]+(\.[0-9]+)?$")
    ).agg(F.sum(F.col("amount").cast("double") * fx_map[key]).alias("d")
          ).collect()[0]["d"]
    return float(total or 0.0)


# ---------------------------------------------------------------------------
# per-class OUTPUT residuals (read the agent's table)
# ---------------------------------------------------------------------------
def d8_revenue_undercount(out_df, contract: Dict[str, Any], true_total: float):
    """D8: does the shipped revenue total reconcile to the true total?"""
    from pyspark.sql import functions as F
    col = contract["revenue_col"]
    shipped = out_df.agg(F.sum(F.col(col).cast("double")).alias("s")).collect()[0]["s"]
    shipped = float(shipped or 0.0)
    dropped = true_total - shipped
    present = dropped > RECON_TOL          # output under-counts => value silently dropped
    return {
        "dollars_dropped": (dropped if present else 0.0),
        "shipped_total": shipped, "true_total": true_total,
        "reconciles": abs(dropped) <= RECON_TOL,
    }, present


def d2_impossible_dates(out_df, contract: Dict[str, Any]):
    """D2: rows bucketed to an impossible far-future date (epoch-ms misparse)."""
    from pyspark.sql import functions as F
    col = contract["date_col"]
    n = out_df.filter(F.year(F.col(col)) > 9999).count()
    return {"impossible_date_rows": n}, (n > 0)


def d7_wrong_day_buckets(out_df, contract: Dict[str, Any], spark, input_path: str, substrate: str):
    """D7 (coarse live signal): the output buckets rows onto calendar DAYS that
    do not exist under correct UTC bucketing.

    A session-local-tz day boundary pushes rows onto an adjacent day, so the
    output's day set acquires day(s) the UTC-correct set does not have. We count
    those invented days as the residual. This is a substrate-agnostic membership
    check; the precise per-row count is the battery quantifier (q_d7 / pay_d7),
    which is the authoritative D7 oracle and is unit-tested. revenue_col is not
    required here.
    """
    date_col = contract["date_col"]
    shipped_days = {r[date_col] for r in out_df.select(date_col).distinct().collect()
                    if r[date_col] is not None}
    if substrate == "orders":
        truth = _orders_utc_day_totals(spark, input_path)
    elif substrate == "payments":
        truth = _payments_utc_day_counts(spark, input_path)
    else:
        return {"wrong_day_buckets": 0, "note": "D7 day-truth not built for this substrate"}, False
    truth_days = set(truth.keys())
    invented = shipped_days - truth_days
    return {"wrong_day_buckets": len(invented), "output_days": len(shipped_days),
            "true_days": len(truth_days)}, (len(invented) > 0)


def _orders_utc_day_totals(spark, input_path: str) -> Dict[Any, float]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("order_id", StringType()), StructField("merchant_id", StringType()),
        StructField("event_time", StringType()), StructField("amount", StringType()),
        StructField("category", StringType()),
    ])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df = spark.read.text(input_path).select(F.from_json("value", schema).alias("s")).select(
        F.col("s.event_time").alias("et"), F.col("s.amount").alias("amt"))
    # only ISO rows contribute a real day; epoch-ms rows are a separate (D2) issue
    iso = df.filter(F.col("et").rlike(r"^\d{4}-\d{2}-\d{2}T")) \
            .filter(F.col("amt").rlike(r"^-?[0-9]+(\.[0-9]+)?$")) \
            .select(F.to_date(F.col("et")).alias("d"), F.col("amt").cast("double").alias("a"))
    return {r["d"]: float(r["s"] or 0.0) for r in
            iso.groupBy("d").agg(F.sum("a").alias("s")).collect()}


def _payments_utc_day_counts(spark, input_path: str) -> Dict[Any, float]:
    """UTC-correct settlement day -> count of valid payments (D7 day-truth)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    schema = StructType([
        StructField("payment_id", StringType()), StructField("account_id", StringType()),
        StructField("event_time", TimestampType()), StructField("currency", StringType()),
        StructField("amount_minor", StringType()), StructField("amount", StringType()),
        StructField("settled", StringType()),
    ])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df = spark.read.text(input_path).select(
        F.from_json("value", schema).getField("event_time").alias("ts")).filter(F.col("ts").isNotNull())
    return {r["d"]: float(r["n"]) for r in
            df.select(F.to_date("ts").alias("d")).groupBy("d").agg(F.count(F.lit(1)).alias("n")).collect()}


def d6_arbitrary_survivor(out_df, contract: Dict[str, Any], spark, input_path: str, substrate: str):
    """D6: the surviving row per key is not the deterministic (latest-seq) one."""
    key = contract.get("key_col")
    payload = contract.get("payload_cols")
    if not key or not payload:
        return {"arbitrary_survivors": 0, "note": "no key/payload contract"}, False
    if substrate == "cdc":
        truth = _cdc_latest_by_seq(spark, input_path)   # key -> (tier, region)
    elif substrate == "orders":
        truth = _orders_latest_by_event_time(spark, input_path, key, payload)
    else:
        return {"arbitrary_survivors": 0,
                "note": f"D6 survivor-truth not built for {substrate}"}, False
    shipped = {r[key]: tuple(r[p] for p in payload) for r in
               out_df.select(key, *payload).collect()}
    wrong = sum(1 for k, v in shipped.items() if k in truth and truth[k] != v)
    return {"arbitrary_survivors": wrong}, (wrong > 0)




def _orders_latest_by_event_time(spark, input_path: str, key_col: str, payload_cols: List[str]) -> Dict[str, tuple]:
    """Deterministic orders D6 truth: one survivor per order_id, choosing the row
    with the greatest parsed event_time (UTC). Payload values are normalized to
    strings to match the task contract's raw amount/category payload columns.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("order_id", StringType()), StructField("merchant_id", StringType()),
        StructField("event_time", StringType()), StructField("amount", StringType()),
        StructField("category", StringType()),
    ])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df = spark.read.text(input_path).select(F.from_json("value", schema).alias("s")).select(
        "s.order_id", "s.event_time", "s.amount", "s.category")
    is_epoch = F.col("event_time").rlike(r"^[0-9]+$")
    ts = F.when(is_epoch, F.timestamp_millis(F.col("event_time").cast("long"))) \
          .otherwise(F.to_timestamp("event_time"))
    w = Window.partitionBy(key_col).orderBy(F.col("_ts").desc_nulls_last())
    latest = df.withColumn("_ts", ts).filter(F.col(key_col).isNotNull()) \
               .withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    return {r[key_col]: tuple(r[p] for p in payload_cols) for r in
            latest.select(key_col, *payload_cols).collect()}

def _cdc_latest_by_seq(spark, input_path: str) -> Dict[str, tuple]:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("customer_id", StringType()), StructField("name", StringType()),
        StructField("tier", StringType()), StructField("region", StringType()),
        StructField("op", StringType()), StructField("seq", StringType()),
        StructField("event_time", StringType()),
    ])
    df = spark.read.text(input_path).select(F.from_json("value", schema).alias("s")).select(
        "s.customer_id", "s.tier", "s.region", "s.op", F.col("s.seq").cast("long").alias("seq"))
    w = Window.partitionBy("customer_id").orderBy(F.col("seq").desc())
    latest = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1) \
               .filter(F.col("op") != "D")
    return {r["customer_id"]: (r["tier"], r["region"]) for r in
            latest.select("customer_id", "tier", "region").collect()}


# ---------------------------------------------------------------------------
# build the OutputProfile from a completed run's materialized table(s)
# ---------------------------------------------------------------------------
def build_output_profile(read_table: Optional[Callable[[str], Any]], spark, input_path: str,
                         defects_in_scope: List[str],
                         contract: Optional[Dict[str, Any]],
                         read_path: Optional[Callable[[str], Any]] = None,
                         output_path: Optional[str] = None,
                         dedup_path: Optional[str] = None) -> oraclesmod.OutputProfile:
    """Read the agent's materialized output and compute residual corruption.

    `read_table(name) -> DataFrame` is the executor's real catalog reader for SDP /
    remote Connect outputs. Imperative LOCAL passes `read_path(path) -> DataFrame`
    plus `output_path`, so the same blind oracle grades the same logical GOLD dataset
    without touching a catalog or Hive metastore.

    `dedup_path` extends that same grade-from-DISK philosophy to the D6 SECONDARY
    dedup table when it is a SEPARATE dataset from the primary output (e.g.
    dedup_table=silver_orders/clean_orders while table=gold_daily). LOCAL IMPERATIVE
    materializes that table to its OWN parquet path (AGENT_DEDUP_PATH) and passes
    `dedup_path` here so D6 reads it from disk -- never via the live session catalog,
    which cannot see it after the agent's idiomatic `spark.stop()`. SDP / remote
    Connect pass no `read_path`/`dedup_path` and keep their live-catalog read.
    """
    prof = oraclesmod.OutputProfile()
    if not contract:
        prof.extra["no_contract"] = True
        return prof
    substrate = contract.get("substrate", "orders")
    try:
        if read_path is not None and output_path:
            out_df = read_path(output_path)
            prof.extra["output_path"] = output_path
        elif read_table is not None:
            out_df = read_table(contract["table"])
        else:
            prof.extra["output_read_error"] = "no output reader provided"
            return prof
    except Exception as e:  # noqa: BLE001
        prof.extra["output_read_error"] = str(e)
        return prof

    true_total = None
    if "D8" in defects_in_scope and contract.get("revenue_col"):
        true_total = (_payments_true_usd_total(spark, input_path) if substrate == "payments"
                      else _orders_true_total(spark, input_path))
        d8, present = d8_revenue_undercount(out_df, contract, true_total)
        prof.d8_dollars_dropped = float(d8["dollars_dropped"])
        prof.d8_rows_dropped = 1 if present else 0   # presence count; dollars carry the magnitude
        prof.reconciles = d8["reconciles"]
        prof.extra["d8"] = d8
    if "D2" in defects_in_scope and contract.get("date_col"):
        d2, _ = d2_impossible_dates(out_df, contract)
        prof.d2_misparsed_rows = int(d2["impossible_date_rows"])
        prof.extra["d2"] = d2
    if "D7" in defects_in_scope and contract.get("date_col"):
        d7, _ = d7_wrong_day_buckets(out_df, contract, spark, input_path, substrate)
        prof.d7_wrong_day_rows = int(d7["wrong_day_buckets"])
        prof.extra["d7"] = d7
    if "D6" in defects_in_scope and contract.get("key_col"):
        # D6 grades the dedup/current-state table, which may differ from the
        # revenue table used for D2/D7/D8.
        d6_table = contract.get("dedup_table", contract["table"])
        try:
            # Path-based imperative output has a parquet final GOLD dataset. D6 may
            # target a SEPARATE dedup/current-state table:
            #   * same table as the primary output -> already read from disk above.
            #   * LOCAL IMPERATIVE separate table   -> materialized to its OWN parquet
            #     path (AGENT_DEDUP_PATH) and read from DISK via `dedup_path`. This
            #     mirrors the primary gold read-back fix: the grade must NOT depend on
            #     the agent's session catalog, which loses the table when the agent
            #     calls `spark.stop()` (a revived session sees no in-session view).
            #   * SDP / remote Connect              -> no read_path/dedup_path; read it
            #     back from the LIVE catalog via read_table (those arms grade in an
            #     isolated subprocess whose session is still alive). Unchanged.
            if read_path is not None and d6_table == contract["table"]:
                d6_df = out_df
            elif read_path is not None and dedup_path:
                d6_df = read_path(dedup_path)
                prof.extra["d6_dedup_path"] = dedup_path
            elif read_table is not None:
                d6_df = read_table(d6_table)
            else:
                prof.extra["d6_read_error"] = (
                    f"no reader for required graded table {d6_table!r}; the task "
                    "contract requires this table to be materialized")
                prof.extra["required_output_read_error"] = prof.extra["d6_read_error"]
                d6_df = None
            if d6_df is None:
                raise RuntimeError(prof.extra["d6_read_error"])
            d6, _ = d6_arbitrary_survivor(d6_df, contract, spark, input_path, substrate)
            prof.d6_ambiguous_keys_unhandled = int(d6["arbitrary_survivors"])
            prof.extra["d6"] = d6
        except Exception as e:  # noqa: BLE001
            prof.extra["d6_read_error"] = str(e)
            prof.extra["required_output_read_error"] = str(e)
    return prof
