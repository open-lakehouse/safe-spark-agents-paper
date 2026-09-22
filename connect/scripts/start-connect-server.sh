#!/usr/bin/env bash
# start-connect-server.sh — launch the OSS Apache Spark Connect server as a first-class service.
#
# Promotes the benchmark-embedded one-liner (scripts/bench_clean.sh) into a durable,
# parameterized launcher. Writes a PID file, redirects logs, blocks until the gRPC port is
# accepting TCP, then prints the sc:// URL. Refuses to start if the port is already bound.
#
# Two modes:
#   (default)      fork the JVM, return once the port is ready (good for dev / scripts).
#   --foreground   exec the JVM in the foreground so a supervisor (systemd) owns it directly;
#                  a background reporter still prints the sc:// URL once the port is ready.
#
# Every knob has a flag and an env default (flag wins). See --help.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- defaults (env overridable; flags override env) -----------------------------------------
port="${SPARK_CONNECT_PORT:-15002}"
warehouse_dir="${SPARK_CONNECT_WAREHOUSE_DIR:-/srv/spark/warehouse}"
driver_memory="${SPARK_CONNECT_DRIVER_MEMORY:-20g}"   # sized for an r7i.xlarge (32 GiB) box
jars="${SPARK_CONNECT_JARS:-}"                          # empty => glob "$REPO_ROOT"/connect/jars/*.jar
conf_dir="${SPARK_CONF_DIR:-$REPO_ROOT/deploy/connect-server/conf}"
pid_file="${SPARK_CONNECT_PID_FILE:-/srv/spark/run/spark-connect.pid}"
log_file="${SPARK_CONNECT_LOG_FILE:-/srv/spark/logs/spark-connect.log}"
advertise_host="${SPARK_CONNECT_HOST:-127.0.0.1}"      # host used only for the printed sc:// URL
readiness_timeout="${SPARK_CONNECT_READY_TIMEOUT:-90}" # seconds to wait for the gRPC port
# Pre-shared bearer token. OFF by default: this is coarse server-wide auth, NOT per-user authz.
# Real per-user auth is a separate authenticating-proxy task. Enable for a quick shared secret:
#   SPARK_CONNECT_AUTHENTICATE_TOKEN=... start-connect-server.sh
token="${SPARK_CONNECT_AUTHENTICATE_TOKEN:-}"
foreground=false

usage() {
  cat <<EOF
Usage: ${0##*/} [options]

Launch the Spark Connect server (org.apache.spark.sql.connect.service.SparkConnectServer).

Options:
  --port PORT              gRPC binding port            (default: ${port})
  --warehouse-dir DIR      spark.sql.warehouse.dir      (default: ${warehouse_dir})
  --driver-memory MEM      driver heap, e.g. 20g        (default: ${driver_memory})
  --jars CSV               extra jars (comma-separated)  (default: glob ${REPO_ROOT}/connect/jars/*.jar)
  --conf-dir DIR           SPARK_CONF_DIR for defaults  (default: ${conf_dir})
  --pid-file PATH          PID file path                (default: ${pid_file})
  --log-file PATH          log file path (fork mode)    (default: ${log_file})
  --host HOST              host shown in the sc:// URL  (default: ${advertise_host})
  --timeout SECONDS        readiness wait budget        (default: ${readiness_timeout})
  --foreground             exec the JVM in the foreground (for systemd supervision)
  -h, --help               show this help

Environment (read as defaults; the matching flag overrides):
  SPARK_HOME, JAVA_HOME, SPARK_CONNECT_PORT, SPARK_CONNECT_WAREHOUSE_DIR,
  SPARK_CONNECT_DRIVER_MEMORY, SPARK_CONNECT_JARS, SPARK_CONF_DIR,
  SPARK_CONNECT_PID_FILE, SPARK_CONNECT_LOG_FILE, SPARK_CONNECT_HOST,
  SPARK_CONNECT_READY_TIMEOUT, SPARK_CONNECT_AUTHENTICATE_TOKEN
EOF
}

# --- arg parsing ----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)          port="$2"; shift 2 ;;
    --warehouse-dir) warehouse_dir="$2"; shift 2 ;;
    --driver-memory) driver_memory="$2"; shift 2 ;;
    --jars)          jars="$2"; shift 2 ;;
    --conf-dir)      conf_dir="$2"; shift 2 ;;
    --pid-file)      pid_file="$2"; shift 2 ;;
    --log-file)      log_file="$2"; shift 2 ;;
    --host)          advertise_host="$2"; shift 2 ;;
    --timeout)       readiness_timeout="$2"; shift 2 ;;
    --foreground|--no-fork) foreground=true; shift ;;
    -h|--help)       usage; exit 0 ;;
    *) echo "ERROR: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# --- resolve SPARK_HOME / JAVA_HOME ---------------------------------------------------------
if [[ -z "${SPARK_HOME:-}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    SPARK_HOME="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))' 2>/dev/null || true)"
  fi
fi
if [[ -z "${SPARK_HOME:-}" || ! -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  echo "ERROR: SPARK_HOME not set or no spark-submit under it (looked at '${SPARK_HOME:-<unset>}')." >&2
  echo "       Set SPARK_HOME, or install pyspark so it can be derived." >&2
  exit 1
fi
export SPARK_HOME
if [[ -n "${JAVA_HOME:-}" ]]; then
  export JAVA_HOME
fi

# --- defaults that depend on parsed values --------------------------------------------------
if [[ -z "$jars" ]]; then
  shopt -s nullglob
  jar_glob=("$REPO_ROOT"/connect/jars/*.jar)
  shopt -u nullglob
  if [[ ${#jar_glob[@]} -gt 0 ]]; then
    IFS=, jars="${jar_glob[*]}"; unset IFS
  fi
fi

if [[ -d "$conf_dir" ]]; then
  export SPARK_CONF_DIR="$conf_dir"
else
  echo "WARN: conf dir '$conf_dir' not found; relying on SPARK_HOME/conf defaults." >&2
fi

# --- helpers --------------------------------------------------------------------------------
# Probe a local TCP port using bash's /dev/tcp. Returns 0 if something is accepting.
port_open() {
  local p="$1"
  (exec 3<>"/dev/tcp/127.0.0.1/${p}") 2>/dev/null || return 1
  exec 3>&- 3<&-
  return 0
}

# Block until the gRPC port accepts TCP, or the timeout elapses.
wait_for_port() {
  local deadline=$(( SECONDS + readiness_timeout ))
  while (( SECONDS < deadline )); do
    if port_open "$port"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# --- idempotency: refuse to start if the port is already bound ------------------------------
if port_open "$port"; then
  echo "ERROR: port ${port} is already accepting connections — a Connect server appears to be running." >&2
  echo "       Stop it first (connect/scripts/stop-connect-server.sh) or pick another --port." >&2
  exit 1
fi

# --- prepare runtime dirs (best effort; systemd's 'spark' user owns these in prod) ----------
mkdir -p "$(dirname "$pid_file")" 2>/dev/null || true
mkdir -p "$(dirname "$log_file")" 2>/dev/null || true
# Create the warehouse dir only when it is a local path (no URI scheme like hdfs:// or s3://).
if [[ "$warehouse_dir" != *"://"* ]]; then
  mkdir -p "$warehouse_dir" 2>/dev/null || true
fi

# --- build the spark-submit command ---------------------------------------------------------
cmd=(
  "${SPARK_HOME}/bin/spark-submit"
  --class org.apache.spark.sql.connect.service.SparkConnectServer
  --name "Spark Connect Server"
  --driver-memory "$driver_memory"
  --conf "spark.connect.grpc.binding.port=${port}"
  --conf "spark.sql.warehouse.dir=${warehouse_dir}"
)
if [[ -n "$jars" ]]; then
  cmd+=( --jars "$jars" )
fi
if [[ -n "$token" ]]; then
  # NOTE: token is passed via --conf; it can appear in the process table. Prefer SPARK_CONF_DIR
  # for anything more than a throwaway shared secret. See the README.
  cmd+=( --conf "spark.connect.authenticate.token=${token}" )
fi
cmd+=( spark-internal )

connect_url="sc://${advertise_host}:${port}"

# --- launch ---------------------------------------------------------------------------------
if [[ "$foreground" == true ]]; then
  # Foreground (systemd Type=notify): we do NOT exec the JVM, because after exec nothing could
  # run the readiness loop or signal systemd — the service would be marked "started" the instant
  # exec succeeds, before the gRPC port is listening. Instead we:
  #   (a) start the JVM as a child and record its PID,
  #   (b) run the readiness loop,
  #   (c) signal systemd READY=1 only once the port accepts TCP (via systemd-notify),
  #   (d) wait on the child so systemd still supervises the real JVM (it shares our cgroup,
  #       and Restart=always restarts us — and thus the JVM — when it exits).
  # Logs go to stdout/stderr (journald).
  echo "Starting Spark Connect server (foreground) on port ${port}..."
  "${cmd[@]}" &
  server_pid=$!
  echo "$server_pid" > "$pid_file"

  # Forward termination to the JVM so it stops gracefully when systemd stops the unit, and
  # always drop the (foreground) PID file when we exit, however we exit.
  # shellcheck disable=SC2317  # invoked asynchronously via trap
  forward_term() { kill -TERM "$server_pid" 2>/dev/null || true; }
  # shellcheck disable=SC2317  # invoked asynchronously via trap
  cleanup_pidfile() { rm -f "$pid_file" 2>/dev/null || true; }
  trap forward_term TERM INT
  trap cleanup_pidfile EXIT

  if wait_for_port; then
    echo "Spark Connect server ready at ${connect_url} (pid ${server_pid})"
    # Notify systemd only under Type=notify (NOTIFY_SOCKET is set by systemd then). Harmless
    # no-op for plain CLI foreground runs.
    if [[ -n "${NOTIFY_SOCKET:-}" ]] && command -v systemd-notify >/dev/null 2>&1; then
      systemd-notify --ready 2>/dev/null || true
    fi
  else
    echo "WARN: gRPC port ${port} not ready after ${readiness_timeout}s; the JVM is still supervised (systemd's TimeoutStartSec will decide)." >&2
  fi

  # Supervise the actual JVM: block until it exits, then propagate its status. The loop guards
  # against 'wait' returning early when our trap fires on a delivered signal.
  status=0
  while kill -0 "$server_pid" 2>/dev/null; do
    wait "$server_pid"
    status=$?
  done
  exit "$status"  # cleanup_pidfile runs via the EXIT trap
fi

# Fork mode: launch detached, record the PID, redirect logs, then block on readiness.
echo "Starting Spark Connect server on port ${port} (logs: ${log_file})..."
"${cmd[@]}" >>"$log_file" 2>&1 &
server_pid=$!
echo "$server_pid" > "$pid_file"

if wait_for_port; then
  echo "Spark Connect server ready at ${connect_url} (pid ${server_pid})"
  exit 0
fi

echo "ERROR: gRPC port ${port} did not come up within ${readiness_timeout}s." >&2
echo "       Last log lines (${log_file}):" >&2
tail -n 20 "$log_file" >&2 2>/dev/null || true
# Clean up the half-started process so we don't leak it.
if kill -0 "$server_pid" 2>/dev/null; then
  kill -TERM "$server_pid" 2>/dev/null || true
fi
rm -f "$pid_file"
exit 1
