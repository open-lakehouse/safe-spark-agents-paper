# Spark Connect server — durable, systemd-managed service

This promotes the Spark Connect launch out of an earlier benchmark helper (where it was a
fire-and-forget one-liner) into a first-class service with a parameterized launcher, a stop
script, hardened server-side defaults, and a systemd unit.

OSS Apache Spark 4.1.1 + JDK 17. The server class is
`org.apache.spark.sql.connect.service.SparkConnectServer`, shipped inside the Spark/PySpark
distribution; the repo's `connect/jars/` (Kafka connectors) are added via `--jars`.

## Files

| File | Purpose |
|---|---|
| `../../connect/scripts/start-connect-server.sh` | parameterized launcher; fork or `--foreground` |
| `../../connect/scripts/stop-connect-server.sh`  | graceful stop (PID file → port → pgrep) |
| `conf/spark-defaults.conf`              | durable server-side defaults + hardening |
| `spark-connect.service`                 | systemd unit (supervises the JVM directly) |

## Run locally for dev

From a checkout, with a JDK 17 on `JAVA_HOME` and `pyspark` importable (or `SPARK_HOME` set):

```bash
# Foreground or fork? Default forks, waits for the port, prints the URL, returns.
connect/scripts/start-connect-server.sh \
  --port 15002 \
  --warehouse-dir "$PWD/_warehouse" \
  --driver-memory 4g

# ... it prints:  Spark Connect server ready at sc://127.0.0.1:15002 (pid NNNN)

connect/scripts/stop-connect-server.sh
```

`SPARK_HOME` is auto-derived from the installed `pyspark` if unset. Everything has a flag and a
matching env var (flag wins) — see `start-connect-server.sh --help`.

Re-running while it's up is safe: the launcher refuses to start if the port is already bound.

## How the systemd unit is installed (by the AWS user-data)

The unit is **not** installed by anything in this directory — `deploy/aws/` user-data does it at
instance boot. The expected layout the unit assumes:

```text
/opt/safe-spark-agents/        # the repo checkout (connect/, deploy/)
/opt/spark/                    # the Spark distribution         -> SPARK_HOME
/srv/spark/                    # owned by the spark user
  warehouse/                   # spark.sql.warehouse.dir
  run/spark-connect.pid
  logs/spark-connect.log
```

Roughly what the user-data does:

```bash
useradd --system --home-dir /srv/spark --shell /usr/sbin/nologin spark
mkdir -p /srv/spark/{warehouse,run,logs}
chown -R spark:spark /srv/spark

# Production source of the deployment values (the unit reads it via EnvironmentFile=).
# JAVA_HOME here is the AMI's JDK (e.g. Amazon Corretto) — the unit does NOT hardcode a distro path.
cat >/etc/spark-connect.env <<'ENV'
JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
SPARK_HOME=/opt/spark
SPARK_CONNECT_PORT=15002
SPARK_CONNECT_WAREHOUSE_DIR=/srv/spark/warehouse
SPARK_CONNECT_DRIVER_MEMORY=20g
ENV
chmod 0644 /etc/spark-connect.env

install -m 0644 /opt/safe-spark-agents/deploy/connect-server/spark-connect.service \
  /etc/systemd/system/spark-connect.service
systemctl daemon-reload
systemctl enable --now spark-connect.service
```

`/etc/spark-connect.env` is the single production source of `JAVA_HOME`, `SPARK_HOME`,
`SPARK_CONNECT_PORT`, `SPARK_CONNECT_WAREHOUSE_DIR`, and `SPARK_CONNECT_DRIVER_MEMORY` (and,
optionally, `SPARK_CONNECT_AUTHENTICATE_TOKEN`). The unit ships only generic fallback defaults
via `Environment=`; the `EnvironmentFile=-/etc/spark-connect.env` line is read afterward and
overrides them. The `-` prefix makes the file optional, so the unit still works on a dev box
without it (the launcher derives `SPARK_HOME` from pyspark and uses the system `java`).

Operate it like any service:

```bash
systemctl status spark-connect
journalctl -u spark-connect -f      # logs go to the journal
systemctl restart spark-connect     # Restart=always, RestartSec=10 also auto-recovers crashes
```

The unit is **`Type=notify`** and runs `start-connect-server.sh --foreground`. The launcher
starts the JVM as a child, runs the readiness loop, and signals `systemd-notify --ready` **only
once the gRPC port accepts TCP** — so `systemctl start` blocks until the server is actually
reachable, not just until the JVM process spawned. It then `wait`s on the child, so the real JVM
(in this unit's cgroup) is what `Restart=always` supervises. A PID file is written in foreground
mode too (`SPARK_CONNECT_PID_FILE`).

## Environment / knobs

On the unit these come from `/etc/spark-connect.env` (production) over generic `Environment=`
fallbacks; each is also a launcher flag that overrides the env per-launch:

| Env | Flag | Default | Meaning |
|---|---|---|---|
| `SPARK_HOME` | — | env file / derived from pyspark | Spark distribution |
| `JAVA_HOME` | — | env file (AMI JDK) / system `java` | JDK (no distro path baked in the unit) |
| `SPARK_CONF_DIR` | `--conf-dir` | this `conf/` dir | server-side defaults |
| `SPARK_CONNECT_PORT` | `--port` | `15002` | gRPC binding port |
| `SPARK_CONNECT_WAREHOUSE_DIR` | `--warehouse-dir` | `/srv/spark/warehouse` | warehouse |
| `SPARK_CONNECT_DRIVER_MEMORY` | `--driver-memory` | `20g` | driver heap (r7i.xlarge) |
| `SPARK_CONNECT_JARS` | `--jars` | glob `<repo>/connect/jars/*.jar` | extra jars |
| `SPARK_CONNECT_PID_FILE` | `--pid-file` | `/srv/spark/run/spark-connect.pid` | PID file (both modes) |
| `SPARK_CONNECT_LOG_FILE` | `--log-file` | `/srv/spark/logs/spark-connect.log` | log (fork mode; foreground → journal) |
| `SPARK_CONNECT_HOST` | `--host` | `127.0.0.1` | host in the printed URL |
| `SPARK_CONNECT_READY_TIMEOUT` | `--timeout` | `90` | readiness wait (s) |
| `SPARK_CONNECT_AUTHENTICATE_TOKEN` | — | unset (OFF) | pre-shared bearer token |
| `SPARK_CONNECT_STOP_TIMEOUT` | `--timeout` (stop) | `30` | grace before SIGKILL |

Server hardening defaults (session/execution timeouts, max inbound message size, max plan size)
live in `conf/spark-defaults.conf`; edit there.

## Enabling the pre-shared token

`spark.connect.authenticate.token` is a single bearer token checked on every gRPC call. It is
**coarse, server-wide auth — not per-user authorization.** Real per-user auth (distinct
principals, Unity Catalog grants) is a separate authenticating-proxy task; this token only gates
who can reach the server at all. It is **OFF by default.**

To enable a shared secret:

```bash
# Dev (CLI): the launcher passes it via --conf.
SPARK_CONNECT_AUTHENTICATE_TOKEN="$(openssl rand -hex 32)" connect/scripts/start-connect-server.sh
```

```bash
# Prod (systemd): keep it out of the unit and the process args. Put it in a root-only file
# and let the unit source it.
printf 'SPARK_CONNECT_AUTHENTICATE_TOKEN=%s\n' "$(openssl rand -hex 32)" \
  | sudo tee /etc/spark-connect/auth.env >/dev/null
sudo chmod 600 /etc/spark-connect/auth.env
# then uncomment the EnvironmentFile= line in spark-connect.service and restart.
```

Clients then connect with the token in the URL:
`sc://host:15002/;token=<secret>;use_ssl=true`. Note that passing it via `--conf` puts the
secret in the process table — prefer the `EnvironmentFile=` route in production, and front the
server with TLS termination so the token isn't sent in the clear.

## Verifying without a running Spark

The launcher resolves `SPARK_HOME` and the server class lazily, so static checks don't need a
live cluster:

```bash
shellcheck connect/scripts/start-connect-server.sh connect/scripts/stop-connect-server.sh
bash -n connect/scripts/start-connect-server.sh connect/scripts/stop-connect-server.sh
systemd-analyze verify deploy/connect-server/spark-connect.service
```
