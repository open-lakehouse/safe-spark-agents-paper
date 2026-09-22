"""Extended defect quantifiers for the second/third study substrates.

Same single-source-of-truth discipline and the SAME blind, arm-agnostic style as
`quantify.py` (functions take only `(spark, path)`, never an arm or model). These
measure the SAME pre-registered defect CLASSES (D6/D7/D8) on INDEPENDENT datasets
-- the customer CDC stream and the multi-currency payments stream -- so a per-class
detection rate is corroborated on more than one dataset rather than resting on the
orders stream alone.

  cdc_d6  -- D6 nondeterministic dedup on the CDC stream. Unlike the orders dups
             (byte-identical -> latent, 0 ambiguous), CDC updates carry DIFFERENT
             payloads in shuffled arrival order, so a dedup/SCD without ordering
             by `seq` yields a genuinely arbitrary survivor. This is the NON-latent
             form of D6 the orders dataset cannot show.
  pay_d7  -- D7 timezone/day-bucket on payments. FX-rate "as of the event date"
             is wrong when the date is taken in session-local tz instead of UTC;
             count rows that bucket to a different UTC day than local day.
  pay_d8  -- D8 silent drop on payments. A naive same-currency `sum(amount)` (no
             FX normalization, no quarantine) silently drops/mis-totals foreign,
             bad-currency, null, and string-amount rows. Reports the USD-equivalent
             value silently excluded.

Usage:  python3 quantify_ext.py <cdc_d6|pay_d7|pay_d8> <dataset.ndjson>
Prints one JSON object: {"defect","rows_affected","detail"} to stdout.
"""
import datetime as dt
import importlib.util
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, LongType,
)


# --- single-source daily FX (generators/fx.py) -----------------------------
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


def _fx_keyed_map(days=10):
    """{ '<ccy>|<YYYY-MM-DD>': rate } over the payments date window, for an
    in-Spark as-of-date lookup that mirrors the generator's daily FX exactly."""
    dates = [FX._FX_EPOCH + dt.timedelta(days=i) for i in range(-1, days)]
    table = FX.rate_table(dates)
    return {f"{ccy}|{d.isoformat()}": rate for (ccy, d), rate in table.items()}


def make_spark(session_tz=None):
    b = (
        SparkSession.builder.master("local[2]")
        .appName("defect_quantify_ext")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    if session_tz:
        b = b.config("spark.sql.session.timeZone", session_tz)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


def load_values(spark, path):
    return spark.read.text(path).select(F.col("value").alias("value"))


# --- CDC schema (customers_cdc) --------------------------------------------
CDC_STR = StructType([
    StructField("customer_id", StringType()),
    StructField("name", StringType()),
    StructField("tier", StringType()),
    StructField("region", StringType()),
    StructField("op", StringType()),
    StructField("seq", StringType()),
    StructField("event_time", StringType()),
])

# --- payments schemas -------------------------------------------------------
PAY_STR = StructType([
    StructField("payment_id", StringType()),
    StructField("account_id", StringType()),
    StructField("event_time", StringType()),
    StructField("currency", StringType()),
    StructField("amount_minor", StringType()),
    StructField("amount", StringType()),
    StructField("settled", StringType()),
])
PAY_TS = StructType([
    StructField("payment_id", StringType()),
    StructField("account_id", StringType()),
    StructField("event_time", TimestampType()),  # pay_d7 injected wrong-tz parse
    StructField("currency", StringType()),
    StructField("amount_minor", LongType()),
    StructField("amount", DoubleType()),         # pay_d8 numeric coercion
    StructField("settled", StringType()),
])

# daily FX -> USD is sourced from generators/fx.py (FX, above); USD == 1.0 every day.
KNOWN_CCY = list(FX.BASE_FX.keys())


def q_cdc_d6(spark, path):
    """D6 on CDC: customers whose events carry >1 distinct (tier,region) payload,
    so a dedup/SCD with no `seq` ordering keeps an ARBITRARY (wrong) survivor."""
    df = load_values(spark, path).select(
        F.from_json("value", CDC_STR).alias("j")
    ).select("j.customer_id", "j.tier", "j.region", "j.op")
    grp = df.groupBy("customer_id").agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct("tier", "region").alias("distinct_payloads"),
    )
    before = df.count()
    after = grp.count()
    dup_keys = grp.filter(F.col("n") > 1).count()
    ambiguous = grp.filter(F.col("distinct_payloads") > 1).count()
    return ambiguous, {
        "rows_before_dedup": before,
        "distinct_customers": after,
        "duplicate_keys": dup_keys,
        "ambiguous_keys_conflicting_payload": ambiguous,
        "note": (f"{ambiguous} customers have multiple distinct (tier,region) "
                 "versions in shuffled order -> dedup without ORDER BY seq keeps "
                 "an arbitrary survivor; non-deterministic SCD output"),
    }


def q_pay_d7(spark, path):
    """D7 on payments: rows whose event_time buckets to a different UTC day than
    the session-local day, so an FX-rate-as-of-LOCAL-date picks the wrong day."""
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    parsed = load_values(spark, path).select(
        F.from_json("value", PAY_TS).getField("event_time").alias("ts")
    ).filter(F.col("ts").isNotNull())
    total = parsed.count()
    diff = parsed.select(
        F.to_date("ts").alias("local_day"),
        F.to_utc_timestamp("ts", "America/Los_Angeles").alias("utc_ts"),
    ).select(
        F.col("local_day"), F.to_date("utc_ts").alias("utc_day"),
    ).filter(F.col("local_day") != F.col("utc_day"))
    misbucketed = diff.count()
    return misbucketed, {
        "parsed_rows": total,
        "session_tz": "America/Los_Angeles",
        "rows_bucketed_to_wrong_day_vs_utc": misbucketed,
        "note": "FX-as-of-date uses the wrong calendar day; converted USD silently wrong",
    }


def q_pay_d8(spark, path):
    """D8 on payments: USD-equivalent value silently excluded from a naive
    same-currency `sum(amount)` with no FX normalization and no quarantine.

    Counts the rows a USD-only / un-normalized sum mishandles (foreign currency,
    unknown currency, null amount, string amount that nulls under a numeric
    schema) and reports the true USD-equivalent dollars dropped, converting each
    foreign row at the rate in effect on its OWN UTC date (v3 daily FX)."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # parse the string and numeric views + the UTC date from the same json line in
    # one pass (zipping separate DataFrames by row order is unsafe).
    both = load_values(spark, path).select(
        F.from_json("value", PAY_STR).getField("currency").alias("ccy"),
        F.from_json("value", PAY_STR).getField("amount").alias("raw_amount"),
        F.from_json("value", PAY_TS).getField("amount").alias("num_amount"),
        F.to_date(F.from_json("value", PAY_TS).getField("event_time")).alias("utc_date"),
    ).cache()
    total = both.count()

    foreign_ccy = [c for c in KNOWN_CCY if c != "USD"]
    foreign = both.filter(F.col("ccy").isin(foreign_ccy))
    bad_ccy = both.filter(~F.col("ccy").isin(KNOWN_CCY))
    foreign_rows = foreign.count()
    bad_rows = bad_ccy.count()
    # numeric amount present but currency != USD -> excluded by a USD-only sum.
    foreign_with_amount = foreign.filter(F.col("num_amount").isNotNull())
    n_dropped = foreign_with_amount.count()
    # true USD-equivalent of those foreign rows at the as-of-UTC-date daily rate.
    rate_map = _fx_keyed_map()
    fx_expr = F.create_map([x for k, v in rate_map.items() for x in (F.lit(k), F.lit(v))])
    key = F.concat_ws("|", F.col("ccy"), F.date_format("utc_date", "yyyy-MM-dd"))
    usd_equiv = foreign_with_amount.select(
        (F.col("num_amount") * fx_expr[key]).alias("usd")
    ).agg(F.sum("usd").alias("d")).collect()[0]["d"]
    # rows lost to null/string-amount coercion under the numeric schema
    raw_present_num_null = both.filter(
        F.col("raw_amount").isNotNull() & F.col("num_amount").isNull()
    ).count()
    return n_dropped, {
        "total_rows": total,
        "foreign_currency_rows": foreign_rows,
        "bad_currency_rows": bad_rows,
        "foreign_rows_with_amount_dropped_from_usd_sum": n_dropped,
        "usd_equivalent_silently_excluded": float(usd_equiv or 0.0),
        "nonnull_raw_amount_nulled_by_numeric_schema": raw_present_num_null,
        "note": "naive USD-only / un-normalized sum drops foreign+bad+string rows; daily FX; COMPLETED",
    }


QUANT_EXT = {"cdc_d6": q_cdc_d6, "pay_d7": q_pay_d7, "pay_d8": q_pay_d8}


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in QUANT_EXT:
        sys.stderr.write("usage: quantify_ext.py <cdc_d6|pay_d7|pay_d8> <dataset.ndjson>\n")
        sys.exit(2)
    defect, path = sys.argv[1], sys.argv[2]
    spark = make_spark()
    try:
        affected, detail = QUANT_EXT[defect](spark, path)
        print(json.dumps({"defect": defect.upper(), "rows_affected": affected, "detail": detail}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
