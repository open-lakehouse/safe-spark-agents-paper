"""D2 plain-PySpark arm: wrong type in parse schema, classic local Spark.

Same defect as the SDP arm: from_json with event_time typed TIMESTAMP over the
orders topic (mostly ISO-8601 strings plus some epoch-millis strings). Reads the
orders topic via a batch Kafka read, parses with the wrong schema, and reports
how the epoch-millis rows are silently mis-parsed. Silent-wrong in BOTH arms.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)

J = "__REPO__/connect/jars"
jars = ",".join(
    f"{J}/{x}"
    for x in [
        "spark-sql-kafka-0-10_2.13-4.1.0.jar",
        "spark-token-provider-kafka-0-10_2.13-4.1.0.jar",
        "kafka-clients-3.9.0.jar",
        "commons-pool2-2.12.0.jar",
    ]
)

spark = (
    SparkSession.builder.master("local[2]")
    .appName("d2_plain")
    .config("spark.jars", jars)
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

WRONG = StructType(
    [
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("event_time", TimestampType()),  # injected defect
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
    ]
)
RAW = StructType([StructField("event_time", StringType())])

src = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)
v = src.select(F.col("value").cast("string").alias("v"))
both = v.select(
    F.from_json("v", RAW).getField("event_time").alias("raw"),
    F.from_json("v", WRONG).getField("event_time").alias("ts"),
).cache()

total = both.count()
raw_null = both.filter(F.col("raw").isNull()).count()
ts_null = both.filter(F.col("ts").isNull()).count()
epoch = both.filter(F.col("raw").rlike("^[0-9]+$"))
epoch_n = epoch.count()
# epoch-millis rows mis-parsed as epoch-SECONDS -> far-future year.
future = epoch.filter(F.year(F.col("ts")) > 9999).count()
loss = both.filter(F.col("raw").isNotNull() & F.col("ts").isNull()).count()

print("D2_PLAIN total rows           :", total)
print("D2_PLAIN raw event_time null  :", raw_null)
print("D2_PLAIN ts  event_time null  :", ts_null)
print("D2_PLAIN epoch-millis rows    :", epoch_n)
print("D2_PLAIN epoch rows -> year>9999 (silently mis-parsed):", future)
print("D2_PLAIN nonnull-raw -> null after TIMESTAMP parse:", loss)
print("D2_PLAIN VERDICT: no error, no exception -> silent-wrong")

spark.stop()
