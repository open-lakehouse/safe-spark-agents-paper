"""p4_fanout: one parsed orders stream fans out to two streaming tables."""
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


def _parsed_orders() -> DataFrame:
    """Single shared parse of the Kafka orders stream (the one source)."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ORDERS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    return (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), ORDERS_SCHEMA).alias("d"))
        .select(
            F.col("d.order_id").alias("order_id"),
            F.col("d.merchant_id").alias("merchant_id"),
            F.col("d.category").alias("category"),
            F.col("d.amount").cast("double").alias("amount"),
        )
        .where(F.col("order_id").isNotNull() & F.col("amount").isNotNull())
    )


@dp.table(name="high_value_orders", comment="Fan-out branch: amount > 200.")
def high_value_orders() -> DataFrame:
    return _parsed_orders().where(F.col("amount") > F.lit(200))


@dp.table(name="standard_orders", comment="Fan-out branch: amount <= 200.")
def standard_orders() -> DataFrame:
    return _parsed_orders().where(F.col("amount") <= F.lit(200))
