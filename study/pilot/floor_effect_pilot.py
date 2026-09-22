#!/usr/bin/env python3
"""Floor-effect pilot for the high-complexity / elevated tasks (corpus v3 §0/§11).

Purpose of the gate (spec): before finalizing a hard task, confirm it is
HARD-BUT-NOT-IMPOSSIBLE -- both arms must not *always* fail, or there is no signal.

This environment has no ANTHROPIC_API_KEY and no live Spark Connect cluster, so
the agentic both-paradigm loop cannot run here (deferred; see the PR note). What
we CAN do deterministically -- and what actually answers the floor-effect
question -- is, per task and per seed, build:

  * a REFERENCE-CORRECT solution (paradigm-agnostic PySpark that runs identically
    under the imperative and SDP/Connect substrates), and
  * a REFERENCE-DEFECTIVE solution committing the exact silent defect the task
    targets,

then grade BOTH through the task's deterministic oracle (quantify_hc / output
oracle). The exit outcomes we report:

  * correct  -> PASS   : a correct build exists  => NOT a floor (some arm can win)
  * defective-> CAUGHT : the oracle flags the silent defect => there IS signal

A task is FLAGGED (possible floor / no signal) iff the correct build does NOT pass
OR the defective build is NOT caught. Runs on seeds 42 and 1337 by default.
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)


def _find_repo_root():
    # Mirrors harness/runner.py: study/ sits two levels deep in the paper repo,
    # three in the original layout. Walk up to the dir holding generators/ or .git.
    env = os.environ.get("STUDY_REPO_ROOT")
    if env:
        return os.path.abspath(env)
    d = HERE
    for _ in range(6):
        d = os.path.dirname(d)
        if os.path.isdir(os.path.join(d, "generators")) or os.path.isdir(os.path.join(d, ".git")):
            return d
    return os.path.normpath(os.path.join(STUDY, "..", ".."))


REPO = _find_repo_root()


def _load(name, rel):
    path = os.path.join(REPO, rel)
    if not os.path.exists(path):
        # the battery lives at <repo>/defect_battery in the paper repo,
        # <repo>/experiments/defect_battery in the original working tree
        path = os.path.join(REPO, "defect_battery", os.path.basename(rel))
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


HC = _load("qhc", "experiments/defect_battery/quantify_hc.py")
FX = HC.FX


def spark_session():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("floor_effect_pilot")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def gen(gen_rel, args, out):
    with open(out, "w") as fo, open(out + ".prof", "w") as fe:
        subprocess.run([sys.executable, os.path.join(REPO, gen_rel)] + args,
                       stdout=fo, stderr=fe, check=True)
    return out


# ---------------------------------------------------------------------------
# HC-1: trades + FX feed -> per-currency USD positions
# ---------------------------------------------------------------------------
def _fx_feed_rates(spark, fx_path, pick):
    """{(ccy, effective_date): rate} choosing the latest-seq (correct) or the
    lowest-seq (defective: keeps a superseded revision) survivor per cell."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("currency", StringType()), StructField("rate", DoubleType()),
        StructField("effective_date", StringType()), StructField("effective_time", StringType()),
        StructField("op", StringType()), StructField("seq", LongType())])
    df = spark.read.text(fx_path).select(F.from_json("value", schema).alias("s")).select(
        "s.currency", "s.rate", "s.effective_date", "s.seq")
    order = F.col("seq").desc() if pick == "latest" else F.col("seq").asc()
    w = Window.partitionBy("currency", "effective_date").orderBy(order)
    one = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    return {(r["currency"], r["effective_date"]): r["rate"]
            for r in one.select("currency", "rate", "effective_date").collect()}


def _hc1_positions(spark, trades_path, rates):
    """Per-currency USD position by JOINING trades to a (ccy,date)->rate map."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    schema = StructType([
        StructField("trade_id", StringType()), StructField("account_id", StringType()),
        StructField("currency", StringType()), StructField("notional", StringType()),
        StructField("event_time", TimestampType()), StructField("side", StringType())])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    rate_map = {f"{c}|{d}": v for (c, d), v in rates.items()}
    fx = F.create_map([x for k, v in rate_map.items() for x in (F.lit(k), F.lit(v))])
    j = spark.read.text(trades_path).select(F.from_json("value", schema).alias("s")).select(
        F.col("s.currency").alias("ccy"), F.col("s.notional").alias("notional"),
        F.to_date(F.col("s.event_time")).alias("d"))
    valid = j.filter(F.col("ccy").isin(list(FX.BASE_FX)) &
                     F.col("notional").rlike(r"^-?[0-9]+(\.[0-9]+)?$") & F.col("d").isNotNull())
    key = F.concat_ws("|", F.col("ccy"), F.date_format("d", "yyyy-MM-dd"))
    rows = valid.select("ccy", (F.col("notional").cast("double") * fx[key]).alias("usd")) \
                .groupBy("ccy").agg(F.sum("usd").alias("p")).collect()
    return {r["ccy"]: float(r["p"] or 0.0) for r in rows}


def pilot_hc1(spark, seed, td):
    tr = gen("generators/gen_trades.py", ["--seed", str(seed)], os.path.join(td, f"tr{seed}.ndjson"))
    fxf = gen("generators/gen_fx_rates_cdc.py", ["--seed", str(seed)], os.path.join(td, f"fx{seed}.ndjson"))
    truth = HC.hc1_truth_positions(spark, tr)
    correct = _hc1_positions(spark, tr, _fx_feed_rates(spark, fxf, "latest"))
    defective = _hc1_positions(spark, tr, _fx_feed_rates(spark, fxf, "lowest"))
    ok_c, _ = HC.hc1_reconcile(correct, truth)
    ok_d, dd = HC.hc1_reconcile(defective, truth)
    return _verdict("HC1_fx_trade_ledger", ok_c, (not ok_d),
                    f"defective superseded-rate mismatches {len(dd['mismatched_currencies'])} currencies")


# ---------------------------------------------------------------------------
# HC-2: clickstream -> sessions/funnel + DLQ accounting
# ---------------------------------------------------------------------------
def pilot_hc2(spark, seed, td):
    ck = gen("generators/gen_clickstream.py", ["--seed", str(seed)], os.path.join(td, f"ck{seed}.ndjson"))
    truth = HC.hc2_truth(spark, ck)
    # CORRECT: clean + DLQ accounted, every event kept.
    ok_c, _ = HC.hc2_event_accounting(truth["n_clean"], truth["n_malformed"], truth["n_source_lines"])
    # DEFECTIVE: silently drop malformed (no DLQ) -> accounting breaks.
    ok_d, _ = HC.hc2_event_accounting(truth["n_clean"], 0, truth["n_source_lines"])
    return _verdict("HC2_session_funnel", ok_c, (not ok_d),
                    f"defective drops {truth['n_malformed']} malformed events with no DLQ")


# ---------------------------------------------------------------------------
# p13: CDC windowed -> current master reconciliation
# ---------------------------------------------------------------------------
def _cdc_current(spark, cdc_path, apply_tombstones):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("customer_id", StringType()), StructField("name", StringType()),
        StructField("tier", StringType()), StructField("region", StringType()),
        StructField("op", StringType()), StructField("seq", StringType()),
        StructField("event_time", StringType())])
    df = spark.read.text(cdc_path).select(F.from_json("value", schema).alias("s")).select(
        "s.customer_id", "s.tier", "s.region", "s.op", F.col("s.seq").cast("long").alias("seq"))
    w = Window.partitionBy("customer_id").orderBy(F.col("seq").desc())
    latest = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    if apply_tombstones:
        latest = latest.filter(F.col("op") != "D")
    return {r["customer_id"]: (r["tier"], r["region"])
            for r in latest.select("customer_id", "tier", "region").collect()}


def pilot_p13(spark, seed, td):
    cdc = gen("generators/gen_customers_cdc.py", ["--seed", str(seed)], os.path.join(td, f"cdc{seed}.ndjson"))
    truth, _ = HC.cdc_truth_current(spark, cdc)
    correct = _cdc_current(spark, cdc, apply_tombstones=True)
    defective = _cdc_current(spark, cdc, apply_tombstones=False)   # keeps tombstoned customers
    ok_c = (correct == truth)
    ok_d = (defective == truth)
    extra = len(set(defective) - set(truth))
    return _verdict("p13_cdc_windowed", ok_c, (not ok_d),
                    f"defective keeps {extra} tombstoned customers in current state")


# ---------------------------------------------------------------------------
# p14: FX settlement -> daily USD reconciliation vs external truth
# ---------------------------------------------------------------------------
def _pay_daily(spark, pay_path, flat):
    """Daily USD totals; flat=True uses base FX (ignores the daily drift) -> wrong."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    schema = StructType([
        StructField("payment_id", StringType()), StructField("account_id", StringType()),
        StructField("event_time", TimestampType()), StructField("currency", StringType()),
        StructField("amount_minor", StringType()), StructField("amount", StringType()),
        StructField("settled", StringType())])
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import datetime as dt
    dates = [FX._FX_EPOCH + dt.timedelta(days=i) for i in range(-1, 12)]
    if flat:
        rate_map = {f"{c}|{d.isoformat()}": FX.BASE_FX[c] for d in dates for c in FX.BASE_FX}
    else:
        rate_map = {f"{c}|{d.isoformat()}": r for (c, d), r in FX.rate_table(dates).items()}
    fx = F.create_map([x for k, v in rate_map.items() for x in (F.lit(k), F.lit(v))])
    j = spark.read.text(pay_path).select(F.from_json("value", schema).alias("s")).select(
        F.col("s.currency").alias("ccy"), F.col("s.amount").alias("amount"),
        F.to_date(F.col("s.event_time")).alias("d"))
    valid = j.filter(F.col("ccy").isin(list(FX.BASE_FX)) &
                     F.col("amount").rlike(r"^-?[0-9]+(\.[0-9]+)?$") & F.col("d").isNotNull())
    key = F.concat_ws("|", F.col("ccy"), F.date_format("d", "yyyy-MM-dd"))
    rows = valid.select(F.date_format("d", "yyyy-MM-dd").alias("day"),
                        (F.col("amount").cast("double") * fx[key]).alias("usd")) \
                .groupBy("day").agg(F.sum("usd").alias("t")).collect()
    return {r["day"]: float(r["t"] or 0.0) for r in rows}


def _daily_reconciles(cand, truth, tol=1.0):
    days = set(cand) | set(truth)
    return all(abs(cand.get(d, 0.0) - truth.get(d, 0.0)) <= tol for d in days)


def pilot_p14(spark, seed, td):
    pay = gen("generators/gen_payments.py", ["--seed", str(seed)], os.path.join(td, f"pay{seed}.ndjson"))
    truth = HC.payments_truth_daily_usd(spark, pay)
    correct = _pay_daily(spark, pay, flat=False)
    defective = _pay_daily(spark, pay, flat=True)   # ignores daily FX drift -> wrong totals
    ok_c = _daily_reconciles(correct, truth)
    ok_d = _daily_reconciles(defective, truth)
    off = sum(1 for d in truth if abs(defective.get(d, 0.0) - truth[d]) > 1.0)
    return _verdict("p14_fx_settlement", ok_c, (not ok_d),
                    f"defective flat-FX is off on {off}/{len(truth)} settlement days")


def _verdict(task, correct_pass, defective_caught, note):
    flagged = (not correct_pass) or (not defective_caught)
    return {
        "task": task,
        "correct_exit": "PASS" if correct_pass else "FAIL",
        "defective_exit": "CAUGHT" if defective_caught else "SILENT(uncaught)",
        "floor_flag": flagged,
        "signal": (correct_pass and defective_caught),
        "note": note,
    }


PILOTS = [pilot_hc1, pilot_hc2, pilot_p13, pilot_p14]


def run_pilot(seeds=(42, 1337)):
    import tempfile
    spark = spark_session()
    report = []
    try:
        for seed in seeds:
            with tempfile.TemporaryDirectory() as td:
                for fn in PILOTS:
                    v = dict(fn(spark, seed, td)); v["seed"] = seed
                    report.append(v)
    finally:
        spark.stop()
    return report


def main():
    import json
    report = run_pilot()
    flagged = [r for r in report if r["floor_flag"]]
    for r in report:
        print(f"seed {r['seed']:>5}  {r['task']:<22} correct={r['correct_exit']:<4} "
              f"defective={r['defective_exit']:<16} signal={r['signal']}  -- {r['note']}")
    print(f"\nFLAGGED (possible floor / no signal): {[r['task'] for r in flagged] or 'none'}")
    print(json.dumps({"n_runs": len(report), "n_flagged": len(flagged)}))
    return 1 if flagged else 0


if __name__ == "__main__":
    sys.exit(main())
