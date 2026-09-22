# Connector jars

These are the Apache Spark Kafka connector jars, bundled so the repo runs offline. They are
Apache License 2.0 (the same license as Apache Spark), freely redistributable. They are bundled
because Maven Central was unreachable in the build environment, so `--packages` resolution fails;
committing the jars makes the pipelines runnable without Maven.

| jar | purpose |
|---|---|
| `spark-sql-kafka-0-10_2.13-4.1.0.jar` | the Kafka source/sink for Structured Streaming |
| `spark-token-provider-kafka-0-10_2.13-4.1.0.jar` | Kafka delegation-token provider |
| `kafka-clients-3.9.0.jar` | the Kafka client library |
| `commons-pool2-2.12.0.jar` | connection pooling used by the connector |

Used via `spark.jars` set in `spark-defaults.conf` (loaded through `SPARK_CONF_DIR`), because
`spark.jars` is immutable inside the SDP spec. Where Maven is reachable, you can instead use
`--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.x` and delete these.
