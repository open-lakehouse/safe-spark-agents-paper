"""p2_cdc: hand-rolled SCD Type 1 + Type 2 over a Kafka CDC stream.

OSS Spark 4.1 has NO apply_changes / apply_cdc primitive, so we implement SCD with
window functions over the materialized raw stream.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
)

spark = SparkSession.active()

KAFKA_BOOTSTRAP = "localhost:9092"
CDC_TOPIC = "customers_cdc"

# STRING-typed schema then coerce seq -> long, event_time -> timestamp.
CDC_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("region", StringType(), True),
    StructField("op", StringType(), True),
    StructField("seq", StringType(), True),
    StructField("event_time", StringType(), True),
])


@dp.table(name="customers_cdc_raw", comment="Raw CDC events streamed from Kafka.")
def customers_cdc_raw() -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", CDC_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("d", F.from_json(F.col("json_str"), CDC_SCHEMA))
        .select("d.*")
        .where(F.col("d.customer_id").isNotNull())
    )
    return parsed.select(
        "customer_id", "name", "tier", "region", "op",
        F.col("seq").cast(LongType()).alias("seq"),
        F.to_timestamp("event_time").alias("event_time"),
    )


@dp.materialized_view(
    name="customers_current",
    comment="SCD Type 1: latest record per customer by max seq, excluding deletes.",
)
def customers_current() -> DataFrame:
    raw = spark.read.table("customers_cdc_raw")
    w = Window.partitionBy("customer_id").orderBy(F.col("seq").desc())
    # Use select (not withColumn) so no eager schema analysis is triggered during
    # pipeline graph registration; the dependency stays lazily resolvable.
    latest = (
        raw.select("*", F.row_number().over(w).alias("_rn"))
        .where(F.col("_rn") == 1)
    )
    # Exclude customers whose most-recent op is a delete.
    return latest.where(F.col("op") != F.lit("D")).select(
        "customer_id", "name", "tier", "region", "seq", "event_time"
    )


@dp.materialized_view(
    name="customers_history",
    comment="SCD Type 2: one row per version with valid_from/valid_to/is_current.",
)
def customers_history() -> DataFrame:
    raw = spark.read.table("customers_cdc_raw")
    w = Window.partitionBy("customer_id").orderBy(F.col("seq").asc())
    # Single select, no withColumn, to keep registration-time analysis lazy.
    return raw.select(
        "customer_id", "name", "tier", "region", "op", "seq",
        F.col("event_time").alias("valid_from"),
        F.lead("event_time").over(w).alias("valid_to"),
        F.lead("event_time").over(w).isNull().alias("is_current"),
    )
