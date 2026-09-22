#!/usr/bin/env bash
# stop-connect-server.sh — gracefully stop the Spark Connect server.
#
# Resolution order for the target PID:
#   1. the PID file written by start-connect-server.sh,
#   2. fallback: whatever is listening on the gRPC port (ss / lsof / fuser),
#   3. fallback: pgrep for the SparkConnectServer main class.
#
# Sends SIGTERM, waits up to --timeout, then escalates to SIGKILL.
set -euo pipefail

# --- defaults (env overridable; flags override env) -----------------------------------------
port="${SPARK_CONNECT_PORT:-15002}"
pid_file="${SPARK_CONNECT_PID_FILE:-/srv/spark/run/spark-connect.pid}"
stop_timeout="${SPARK_CONNECT_STOP_TIMEOUT:-30}"

usage() {
  cat <<EOF
Usage: ${0##*/} [options]

Stop the Spark Connect server gracefully (SIGTERM, then SIGKILL after --timeout).

Options:
  --port PORT      gRPC port to look up if no PID file   (default: ${port})
  --pid-file PATH  PID file to read                      (default: ${pid_file})
  --timeout SECS   grace period before SIGKILL           (default: ${stop_timeout})
  -h, --help       show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)     port="$2"; shift 2 ;;
    --pid-file) pid_file="$2"; shift 2 ;;
    --timeout)  stop_timeout="$2"; shift 2 ;;
    -h|--help)  usage; exit 0 ;;
    *) echo "ERROR: unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# Find the PID of whatever is listening on the gRPC port, trying tools in order of preference.
pid_from_port() {
  local p="$1" found=""
  if command -v ss >/dev/null 2>&1; then
    # ss -H -ltnp: -H no header, parse "pid=NNN" out of the users:(...) column.
    found="$(ss -H -ltnp "sport = :${p}" 2>/dev/null | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)"
  fi
  if [[ -z "$found" ]] && command -v lsof >/dev/null 2>&1; then
    found="$(lsof -t -iTCP:"${p}" -sTCP:LISTEN 2>/dev/null | head -1)"
  fi
  if [[ -z "$found" ]] && command -v fuser >/dev/null 2>&1; then
    found="$(fuser "${p}/tcp" 2>/dev/null | tr -s ' ' | sed 's/^ *//' | cut -d' ' -f1)"
  fi
  printf '%s' "$found"
}

# Read a process's command line portably (Linux /proc, otherwise ps).
proc_cmdline() {
  local p="$1"
  if [[ -r "/proc/${p}/cmdline" ]]; then
    tr '\0' ' ' < "/proc/${p}/cmdline" 2>/dev/null
  else
    ps -p "$p" -o args= 2>/dev/null || true
  fi
}

# True only if the PID is actually a Spark Connect server JVM. This is the PID-reuse guard:
# a recycled PID that now belongs to some unrelated process will not match.
is_connect_server() {
  local p="$1"
  proc_cmdline "$p" | grep -qF 'org.apache.spark.sql.connect.service.SparkConnectServer'
}

# True only if the PID is a Spark Connect server JVM that was launched FOR the requested gRPC
# port. The launcher always passes `--conf spark.connect.grpc.binding.port=<port>`, so its
# presence in the command line is positive proof the process belongs to this port. This is what
# lets us trust a PID file even when nothing is (yet) detected listening: a stale/reused PID file
# pointing at a Connect server on a DIFFERENT port will NOT match and is never signaled.
is_connect_server_for_port() {
  local p="$1" wanted="$2" cl
  cl="$(proc_cmdline "$p")"
  printf '%s\n' "$cl" | grep -qF 'org.apache.spark.sql.connect.service.SparkConnectServer' \
    && printf '%s\n' "$cl" | grep -qF "spark.connect.grpc.binding.port=${wanted}"
}

# --- resolve & VALIDATE the target PID (PID-reuse / wrong-instance guard) --------------------
# We only ever signal a process that is BOTH a SparkConnectServer AND tied to the requested
# --port. So a recycled PID (now some unrelated process) or a Connect server on a DIFFERENT
# port can never be killed by accident. The unconstrained `pgrep` fallback is intentionally
# gone — it could match a Connect server belonging to another port/instance.
pid=""
source="none"
port_pid="$(pid_from_port "$port")"

if [[ -f "$pid_file" ]]; then
  file_pid="$(tr -dc '0-9' < "$pid_file" || true)"
  # Signal a PID-file target ONLY with positive proof it is the server for THIS port: its command
  # line must show spark.connect.grpc.binding.port=${port}, AND if something is listening on the
  # port it must be that same PID. Without the proof, treat the PID file as stale (warn + remove)
  # rather than risk killing a Connect server that belongs to a different port.
  if [[ -n "$file_pid" ]] && kill -0 "$file_pid" 2>/dev/null \
       && is_connect_server_for_port "$file_pid" "$port" \
       && { [[ -z "$port_pid" ]] || [[ "$port_pid" == "$file_pid" ]]; }; then
    pid="$file_pid"; source="pid-file"
  elif [[ -n "$file_pid" ]]; then
    echo "WARN: PID file (${file_pid}) is not a live Spark Connect server bound to port ${port}; treating it as stale and removing it." >&2
    rm -f "$pid_file" 2>/dev/null || true
  fi
fi

# Fallback: the process actually listening on the requested port — but only if it really is a
# Connect server (never signal an unrelated listener that happens to hold the port).
if [[ -z "$pid" && -n "$port_pid" ]] && is_connect_server "$port_pid"; then
  pid="$port_pid"; source="port ${port}"
fi

if [[ -z "$pid" ]]; then
  echo "No running Spark Connect server found for port ${port} (nothing validated to stop)."
  rm -f "$pid_file" 2>/dev/null || true
  exit 0
fi

if ! kill -0 "$pid" 2>/dev/null; then
  echo "PID ${pid} (from ${source}) is not alive; cleaning up."
  rm -f "$pid_file" 2>/dev/null || true
  exit 0
fi

# --- graceful stop, then escalate -----------------------------------------------------------
echo "Stopping Spark Connect server pid ${pid} (found via ${source})..."
kill -TERM "$pid" 2>/dev/null || true

deadline=$(( SECONDS + stop_timeout ))
while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do
  sleep 1
done

if kill -0 "$pid" 2>/dev/null; then
  echo "Still alive after ${stop_timeout}s; sending SIGKILL to ${pid}."
  kill -KILL "$pid" 2>/dev/null || true
  sleep 1
fi

if kill -0 "$pid" 2>/dev/null; then
  echo "ERROR: failed to stop pid ${pid}." >&2
  exit 1
fi

rm -f "$pid_file" 2>/dev/null || true
echo "Spark Connect server stopped."
