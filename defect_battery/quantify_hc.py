"""Deterministic ground truth for the high-complexity cross-stage invariants (v3 §7/§9).

These functions compute, from the matched-seed INPUT alone, the truth that the
HC tasks' cross-stage invariants must satisfy, plus the building blocks the
elevated p13/p14 reconciliations need. They are arm-agnostic (input -> truth);
the floor-effect pilot builds reference-correct and reference-defective outputs
and grades them against these.

  HC-1 fx_trade_ledger : true per-currency USD position (as-of-UTC-date daily FX);
                         SCD2 no-overlap predicate; gold/mart reconciliation.
  HC-2 session_funnel  : 30-min-inactivity sessions + per-stage unique-user funnel;
                         event-accounting (clean + DLQ == source, no double-count).
  p13 cdc_windowed     : current master (latest-by-seq, tombstones removed) +
                         activity totals reconcile to source events.
  p14 fx_settlement    : true daily USD settlement totals (the bank's independent
                         view) for the full-outer reconciliation.
"""
import datetime as dt
import importlib.util
import json
import os
import sys


def _load_fx():
    here = os.path.dirname(os.path.abspath(__file__))
    # generators/ sits at the repo root; walk up to whichever parent holds it
    # (STUDY_REPO_ROOT overrides).
    root = os.environ.get("STUDY_REPO_ROOT")
    if not root:
        d = here
        for _ in range(6):
            d = os.path.dirname(d)
            if os.path.isdir(os.path.join(d, "generators")) or os.path.isdir(os.path.join(d, "infra")):
                root = d
                break
        else:
            root = os.path.normpath(os.path.join(here, "..", ".."))
    fxpath = os.path.join(root, "generators", "fx.py")
    if not os.path.exists(fxpath):  # pre-reorganization layout compat
        fxpath = os.path.join(root, "infra", "fx.py")
    spec = importlib.util.spec_from_file_location("infra_fx", fxpath)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


FX = _load_fx()
RECON_TOL = 1.0


def _fx_keyed_map(days=12):
    dates = [FX._FX_EPOCH + dt.timedelta(days=i) for i in range(-1, days)]
    return {f"{c}|{d.isoformat()}": r for (c, d), r in FX.rate_table(dates).items()}


# ---------------------------------------------------------------------------
# HC-1 FX trade ledger
# ---------------------------------------------------------------------------
def hc1_truth_positions(spark, trades_path):
    """{currency: USD position} over VALID trades only (known currency, numeric
    notional), each valued at the daily rate as-of the trade's UTC date. Unknown
    currencies are quarantined (excluded), matching the task contract."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    schema = StructType([
        StructField("trade_id", StringType()), StructField("account_id", StringType()),
        StructField("currency", StringType()), StructField("notional", StringType()),
        StructField("event_time", TimestampType()), StructField("side", StringType()),
    ])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    rate_map = _fx_keyed_map()
    fx_map = F.create_map([x for k, v in rate_map.items() for x in (F.lit(k), F.lit(v))])
    j = spark.read.text(trades_path).select(F.from_json("value", schema).alias("s")).select(
        F.col("s.currency").alias("ccy"), F.col("s.notional").alias("notional"),
        F.to_date(F.col("s.event_time")).alias("utc_date"))
    valid = j.filter(F.col("ccy").isin(list(FX.BASE_FX)) &
                     F.col("notional").rlike(r"^-?[0-9]+(\.[0-9]+)?$"))
    key = F.concat_ws("|", F.col("ccy"), F.date_format("utc_date", "yyyy-MM-dd"))
    rows = valid.select("ccy", (F.col("notional").cast("double") * fx_map[key]).alias("usd")) \
                .groupBy("ccy").agg(F.sum("usd").alias("pos")).collect()
    return {r["ccy"]: float(r["pos"] or 0.0) for r in rows}


def hc1_reconcile(positions, truth, tol=RECON_TOL):
    """Invariant: a candidate {ccy: usd} mart reconciles to the truth positions."""
    ccys = set(positions) | set(truth)
    diffs = {c: round(positions.get(c, 0.0) - truth.get(c, 0.0), 4)
             for c in ccys if abs(positions.get(c, 0.0) - truth.get(c, 0.0)) > tol}
    return (not diffs), {"mismatched_currencies": diffs,
                         "truth_currencies": len(truth), "candidate_currencies": len(positions)}


def scd2_overlap_count(rows, key="currency", frm="effective_from", to="effective_to"):
    """SCD2 no-overlap invariant: number of (key) groups whose validity periods
    overlap. `rows` is an iterable of dicts. `to`=None means open-ended (current)."""
    from collections import defaultdict
    groups = defaultdict(list)
    for r in rows:
        groups[r[key]].append((r[frm], r[to]))
    overlaps = 0
    for k, periods in groups.items():
        periods = sorted(periods, key=lambda p: p[0])
        for (a_f, a_t), (b_f, b_t) in zip(periods, periods[1:]):
            hi = a_t if a_t is not None else b_f  # open-ended end shouldn't precede next
            if a_t is None or hi > b_f:
                overlaps += 1
    return overlaps


# ---------------------------------------------------------------------------
# HC-2 session funnel
# ---------------------------------------------------------------------------
STAGES = ["view", "cart", "checkout", "purchase"]


def _hc2_parse(spark, click_path):
    raw = [r["value"] for r in spark.read.text(click_path).select("value").collect()]
    clean, malformed = [], 0
    for line in raw:
        try:
            o = json.loads(line)
            if not all(k in o for k in ("event_id", "user_id", "event_type", "event_time")):
                raise ValueError
            o["_ts"] = dt.datetime.fromisoformat(o["event_time"])
            clean.append(o)
        except Exception:
            malformed += 1
    return raw, clean, malformed


def hc2_truth(spark, click_path, gap_minutes=30):
    """30-min-inactivity sessions + per-stage unique-user funnel + event accounting."""
    from collections import defaultdict
    raw, clean, malformed = _hc2_parse(spark, click_path)
    by_user = defaultdict(list)
    for e in clean:
        by_user[e["user_id"]].append(e)
    n_sessions = 0
    reached = {s: set() for s in STAGES}
    gap = dt.timedelta(minutes=gap_minutes)
    for uid, evs in by_user.items():
        evs.sort(key=lambda e: e["_ts"])
        prev = None
        for e in evs:
            if prev is None or (e["_ts"] - prev) > gap:
                n_sessions += 1
            prev = e["_ts"]
            if e["event_type"] in reached:
                reached[e["event_type"]].add(uid)
    return {
        "n_source_lines": len(raw),
        "n_clean": len(clean),
        "n_malformed": malformed,
        "n_sessions": n_sessions,
        "funnel_unique_users": {s: len(reached[s]) for s in STAGES},
        "distinct_users": len(by_user),
    }


def hc2_event_accounting(n_clean, n_dlq, n_source):
    """Invariant: clean + DLQ == source (no event silently dropped/double-counted)."""
    ok = (n_clean + n_dlq == n_source)
    return ok, {"n_clean": n_clean, "n_dlq": n_dlq, "n_source": n_source,
                "accounted": n_clean + n_dlq}


# ---------------------------------------------------------------------------
# p13 cdc windowed  &  p14 fx settlement  (elevated reconciliations)
# ---------------------------------------------------------------------------
def cdc_truth_current(spark, cdc_path):
    """{customer_id: (tier, region)} = latest-by-seq, tombstoned customers removed.
    Plus the total source-event count for the activity reconciliation."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("customer_id", StringType()), StructField("name", StringType()),
        StructField("tier", StringType()), StructField("region", StringType()),
        StructField("op", StringType()), StructField("seq", StringType()),
        StructField("event_time", StringType()),
    ])
    df = spark.read.text(cdc_path).select(F.from_json("value", schema).alias("s")).select(
        "s.customer_id", "s.tier", "s.region", "s.op", F.col("s.seq").cast("long").alias("seq"))
    n_events = df.count()
    w = Window.partitionBy("customer_id").orderBy(F.col("seq").desc())
    latest = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1) \
               .filter(F.col("op") != "D")
    cur = {r["customer_id"]: (r["tier"], r["region"])
           for r in latest.select("customer_id", "tier", "region").collect()}
    return cur, n_events


def payments_truth_daily_usd(spark, pay_path):
    """{settlement_date(str): USD total} = the bank's independent view: every valid
    payment FX-converted at the as-of-UTC-date daily rate. p14's reconciliation
    target (and what a correct settlement_daily must equal)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    schema = StructType([
        StructField("payment_id", StringType()), StructField("account_id", StringType()),
        StructField("event_time", TimestampType()), StructField("currency", StringType()),
        StructField("amount_minor", StringType()), StructField("amount", StringType()),
        StructField("settled", StringType()),
    ])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    rate_map = _fx_keyed_map()
    fx_map = F.create_map([x for k, v in rate_map.items() for x in (F.lit(k), F.lit(v))])
    j = spark.read.text(pay_path).select(F.from_json("value", schema).alias("s")).select(
        F.col("s.currency").alias("ccy"), F.col("s.amount").alias("amount"),
        F.to_date(F.col("s.event_time")).alias("d"))
    valid = j.filter(F.col("ccy").isin(list(FX.BASE_FX)) &
                     F.col("amount").rlike(r"^-?[0-9]+(\.[0-9]+)?$") & F.col("d").isNotNull())
    key = F.concat_ws("|", F.col("ccy"), F.date_format("d", "yyyy-MM-dd"))
    rows = valid.select(F.date_format("d", "yyyy-MM-dd").alias("day"),
                        (F.col("amount").cast("double") * fx_map[key]).alias("usd")) \
                .groupBy("day").agg(F.sum("usd").alias("t")).collect()
    return {r["day"]: float(r["t"] or 0.0) for r in rows}


# registry so the corpus can reference invariant oracles as "quantify_hc.<key>"
# and the test harness can resolve them (mirrors quantify.QUANT / quantify_ext.QUANT_EXT).
QUANT_HC = {
    "hc1_positions": hc1_truth_positions,
    "hc1_reconcile": hc1_reconcile,
    "scd2_no_overlap": scd2_overlap_count,
    "hc2_truth": hc2_truth,
    "hc2_event_accounting": hc2_event_accounting,
    "cdc_current": cdc_truth_current,
    "pay_daily_usd": payments_truth_daily_usd,
}


if __name__ == "__main__":
    sys.stderr.write("quantify_hc.py is a library of ground-truth builders; import it.\n")
    sys.exit(2)
