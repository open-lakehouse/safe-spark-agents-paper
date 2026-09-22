"""p3_windows: event-time windowed revenue aggregation (1-hour windows x category)."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.active()

KAFKA_BOOTSTRAP = "localhost:9092"
ORDERS_TOPIC = "orders"

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("category", StringType(), True),
])


@dp.table(
    name="hourly_revenue",
    comment="Revenue per 1-hour event-time window x category (streaming append, watermarked).",
)
def hourly_revenue() -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ORDERS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), ORDERS_SCHEMA).alias("d"))
        .select("d.*")
        .where(F.col("d.order_id").isNotNull())
    )
    # epoch-ms tolerant timestamp + amount cast; drop rows we cannot place in time.
    typed = parsed.select(
        F.col("category"),
        F.col("amount").cast("double").alias("amount"),
        F.when(
            F.col("event_time").rlike("^[0-9]+$"),
            F.timestamp_seconds((F.col("event_time").cast("double") / F.lit(1000.0)).cast("long")),
        ).otherwise(F.expr("try_to_timestamp(event_time)")).alias("event_time"),
    ).where(F.col("event_time").isNotNull())

    # Watermark on the external Kafka-derived stream, then windowed aggregation.
    # Append output mode emits a window only after the watermark passes its end.
    windowed = (
        typed.withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window(F.col("event_time"), "1 hour").alias("w"),
            F.col("category"),
        )
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("*").alias("order_count"),
        )
    )
    return windowed.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        F.col("category"),
        F.col("total_revenue"),
        F.col("order_count"),
    )
