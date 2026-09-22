"""p5_mart: BATCH pipeline consuming another pipeline's published table (p2_cdc)."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


@dp.materialized_view(
    name="customer_segments",
    comment="Customer counts by tier x region, sourced from p2_cdc.customers_current.",
)
def customer_segments() -> DataFrame:
    # Cross-pipeline read of a table published by p2_cdc (fully-qualified, batch).
    current = spark.read.table("p2_cdc.customers_current")
    return (
        current.groupBy("tier", "region")
        .agg(F.count("*").alias("customer_count"))
        .select("tier", "region", "customer_count")
    )
