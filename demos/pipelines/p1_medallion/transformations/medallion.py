"""p1_medallion: bronze -> silver -> gold medallion architecture over messy Kafka orders."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# SparkSession is NOT auto-injected into transformation modules; bind the active one.
spark = SparkSession.active()

KAFKA_BOOTSTRAP = "localhost:9092"
ORDERS_TOPIC = "orders"
MERCHANTS_PATH = "file://__REPO__/generators/merchants.ndjson"

# All-STRING schema; coerce afterwards so epoch-ms / tz / string-amount values are not
# silently nulled by from_json's strict type parsing.
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("category", StringType(), True),
])


def _read_orders_kafka():
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ORDERS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


def _coerce_event_time(col):
    """Handle ISO timestamps, tz-suffixed strings, and epoch-millis as string.

    Use try_to_timestamp (returns NULL instead of throwing under ANSI) so a single
    malformed/epoch-ms value cannot fail the whole micro-batch.
    """
    # epoch millis (all digits) -> seconds; checked first via a pattern guard
    is_epoch = col.rlike("^[0-9]+$")
    epoch_ms = F.timestamp_seconds((col.cast("double") / F.lit(1000.0)).cast("long"))
    iso_ts = F.expr("try_to_timestamp(event_time)")
    return F.when(is_epoch, epoch_ms).otherwise(iso_ts)


@dp.table(name="bronze_orders", comment="Raw parsed orders from Kafka with corrupt flag.")
def bronze_orders() -> DataFrame:
    raw = _read_orders_kafka()
    val = raw.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    parsed = val.withColumn("data", F.from_json(F.col("json_str"), ORDERS_SCHEMA))
    return parsed.select(
        F.col("data.order_id").alias("order_id"),
        F.col("data.merchant_id").alias("merchant_id"),
        F.col("data.event_time").alias("event_time_raw"),
        F.col("data.amount").alias("amount_raw"),
        F.col("data.category").alias("category"),
        F.col("kafka_ts"),
        # _corrupt when JSON failed to parse OR mandatory order_id missing
        (F.col("data").isNull() | F.col("data.order_id").isNull()).alias("_corrupt"),
    )


@dp.table(name="silver_orders", comment="Deduped, typed orders broadcast-joined to merchants.")
def silver_orders() -> DataFrame:
    raw = _read_orders_kafka()
    val = raw.selectExpr("CAST(value AS STRING) AS json_str")
    parsed = (
        val.withColumn("data", F.from_json(F.col("json_str"), ORDERS_SCHEMA))
        .select("data.*")
        .where(F.col("data.order_id").isNotNull())  # noqa: drop unparseable rows
    )
    typed = (
        parsed.withColumn("event_time", _coerce_event_time(F.col("event_time")))
        .withColumn("amount", F.col("amount").cast("double"))
        .where(F.col("event_time").isNotNull())
    )
    # Watermark goes on the EXTERNAL streaming source (Kafka-derived), then dedup.
    deduped = (
        typed.withWatermark("event_time", "10 minutes")
        .dropDuplicatesWithinWatermark(["order_id"])
    )
    merchants = spark.read.json(MERCHANTS_PATH).select(
        F.col("merchant_id"),
        F.col("merchant_name"),
        F.col("region"),
    )
    # Join on the shared column name "merchant_id" (avoids df.<attr> access that would
    # trigger blocked schema analysis inside the pipeline query function).
    return (
        deduped.join(F.broadcast(merchants), on="merchant_id", how="left")
        .select(
            "order_id", "merchant_id", "merchant_name", "region",
            "event_time", "amount", "category",
        )
    )


@dp.materialized_view(name="gold_daily", comment="Daily revenue by date x category.")
def gold_daily() -> DataFrame:
    # Materialized views are batch; read the silver table in batch mode.
    silver = spark.read.table("silver_orders")
    return (
        silver.groupBy(
            F.to_date("event_time").alias("order_date"),
            F.col("category"),
        )
        .agg(
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.count("*").alias("order_count"),
        )
    )
