# Local Spark Connect runtime

Everything the study harness, the five demos, and the `deploy/connect-server` systemd service
need to run an OSS Spark Connect endpoint locally, grouped under one roof:

- `client.py`, `sandbox_smoke.py` — thin `sc://` client plus a positive/negative smoke test.
- `scripts/` — `start-connect-server.sh` / `stop-connect-server.sh`, the parameterized launcher;
  see `deploy/connect-server/README.md` for the server-side defaults and the production systemd unit.
- `jars/` — Kafka connector jars bundled so the repo runs offline (Apache License 2.0); see
  `jars/README.md`.

The functional base is Apache Spark 4.1's Spark Connect server
(`org.apache.spark.sql.connect.service.SparkConnectServer`); nothing here is Databricks-specific.
