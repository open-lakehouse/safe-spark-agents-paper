#!/usr/bin/env bash
#
# Parameterized E3 defect battery (safe_agent_study).
#
# This supersedes the single hardcoded D1-D9 / seed=42 pass in
# experiments/defect_battery/run_battery.sh. Same real mechanism -- one
# self-managed Spark Connect server with the Kafka connector on the classpath,
# the SAME pipelines/cli.py dry-run gate, the SAME quantify.py oracle -- but the
# inputs are now injectable so the battery can be folded into the study sweep:
#
#   --seeds   "42,1337,..."   one deterministic dataset per seed (default: 42)
#   --N       5000            base record count per dataset (default: 5000)
#   --defects "D1,D2,D8"      restrict to a subset of the D1-D9 battery (default: all)
#   --arms    "sdp,plain"     which approach(es) to run (default: sdp)
#   --trials  3               repeat each (defect, seed) N times for stability (default: 1)
#   --out     <path>          results.jsonl (default: ./battery_results.jsonl)
#
# Each emitted row carries seed, trial, defect, approach, stage, exit_code,
# error_class, wall_s, rows_affected so the study's schema can ingest it.
#
# Requires: pyspark 4.1 with the pipelines module, a JDK, the connector jars
# under <repo>/connect/jars/. No Kafka broker (dry-run reads no data; quantify reads the
# generated NDJSON).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STUDY="$(cd "$HERE/.." && pwd)"
# Mirrors harness/runner.py: study/ sits two levels deep in the paper repo, three in the
# original layout. Walk up to the dir holding generators/ or .git (STUDY_REPO_ROOT overrides).
REPO="${STUDY_REPO_ROOT:-}"
if [[ -z "$REPO" ]]; then
  REPO="$STUDY"
  for _ in 1 2 3 4 5 6; do
    REPO="$(dirname "$REPO")"
    if [[ -d "$REPO/generators" || -d "$REPO/.git" ]]; then break; fi
  done
fi
BATTERY="$REPO/defect_battery"                      # recovered E3 variants + quantify.py live here
[[ -d "$BATTERY" ]] || BATTERY="$REPO/experiments/defect_battery"   # original-layout fallback
VARIANTS_DIR="$BATTERY/variants"
QUANTIFY="$BATTERY/quantify.py"

# --- defaults / args -------------------------------------------------------
SEEDS="42"
NROWS="5000"
DEFECTS="D1,D2,D3,D4,D5,D6,D7,D8,D9"
ARMS="sdp"
TRIALS="1"
OUT="$HERE/battery_results.jsonl"
WORK="${BATTERY_WORK:-$STUDY/.work/battery}"
PORT="${CONNECT_PORT:-15055}"

while [ $# -gt 0 ]; do
  case "$1" in
    --seeds)   SEEDS="$2"; shift 2;;
    --N)       NROWS="$2"; shift 2;;
    --defects) DEFECTS="$2"; shift 2;;
    --arms)    ARMS="$2"; shift 2;;
    --trials)  TRIALS="$2"; shift 2;;
    --out)     OUT="$2"; shift 2;;
    --work)    WORK="$2"; shift 2;;
    --port)    PORT="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

REMOTE="sc://localhost:${PORT}"
PYBIN="${PYSPARK_PYTHON:-python3}"
if [ -z "${SPARK_HOME:-}" ]; then
  SPARK_HOME="$("$PYBIN" -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')"
fi
export SPARK_HOME
CLI="$SPARK_HOME/pipelines/cli.py"
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
SPARK_VERSION="$("$PYBIN" -c 'import pyspark;print(pyspark.__version__)' 2>/dev/null || echo unknown)"

JAR_NAMES=(
  "spark-sql-kafka-0-10_2.13-4.1.0.jar"
  "spark-token-provider-kafka-0-10_2.13-4.1.0.jar"
  "kafka-clients-3.9.0.jar"
  "commons-pool2-2.12.0.jar"
)
jar_csv() { local IFS=,; echo "${JAR_NAMES[*]/#/$REPO/connect/jars/}"; }
KJARS="$(jar_csv)"

# defect registry: id | dir | class | quantify-key (or -)
declare -A DIR_OF=(
  [D1]=D1_missing_col [D2]=D2_wrong_type [D3]=D3_unwatermarked_dedup
  [D4]=D4_broken_dag  [D5]=D5_immutable_config [D6]=D6_nondeterministic_dedup
  [D7]=D7_timezone_bug [D8]=D8_absent_quarantine [D9]=D9_unbounded_state
)
declare -A KLASS_OF=(
  [D1]=structural [D2]=semantic [D3]=state [D4]=structural [D5]=structural
  [D6]=semantic [D7]=semantic [D8]=semantic [D9]=state
)
declare -A QKEY_OF=( [D2]=d2 [D6]=d6 [D7]=d7 [D8]=d8 )

log() { printf '[battery] %s\n' "$*" >&2; }

emit_row() {
  SEED="$1" TRIAL="$2" DEFECT="$3" APPROACH="$4" STAGE="$5" EXIT_CODE="$6" \
  ERROR_CLASS="$7" WALL_S="$8" ROWS_AFFECTED="$9" VERDICT="${10}" \
  "$PYBIN" - <<'PY' >> "$OUT"
import json, os
ra = os.environ["ROWS_AFFECTED"]
row = {
    "seed": int(os.environ["SEED"]),
    "trial": int(os.environ["TRIAL"]),
    "defect": os.environ["DEFECT"],
    "approach": os.environ["APPROACH"],
    "stage": os.environ["STAGE"],
    "exit_code": int(os.environ["EXIT_CODE"]),
    "error_class": os.environ["ERROR_CLASS"] or None,
    "wall_s": float(os.environ["WALL_S"]),
    "rows_affected": (int(ra) if ra not in ("", "-") else None),
    "verdict": os.environ["VERDICT"],
}
print(json.dumps(row))
PY
}

SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

start_server() {
  log "starting Spark Connect server on :$PORT (kafka on classpath, isolation off)"
  "$SPARK_SUBMIT" --master "local[2]" --deploy-mode client \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --name SparkConnectBattery --jars "$KJARS" \
    --conf "spark.connect.grpc.binding.port=${PORT}" \
    --conf "spark.ui.enabled=false" \
    --conf "spark.sql.artifact.isolation.enabled=false" \
    spark-internal > "$WORK/connect-server.log" 2>&1 &
  SERVER_PID=$!
  local i
  for i in $(seq 1 90); do
    grep -q "Spark Connect server started" "$WORK/connect-server.log" 2>/dev/null && { log "up after ${i}s"; return 0; }
    kill -0 "$SERVER_PID" 2>/dev/null || { log "ERROR: server died"; tail -20 "$WORK/connect-server.log" >&2; return 1; }
    sleep 1
  done
  log "ERROR: server did not start in 90s"; return 1
}

build_variant() {
  local dir="$1" dst="$2"
  rm -rf "$dst"; cp -r "$VARIANTS_DIR/$dir" "$dst"
  find "$dst" -type f \( -name '*.yml' -o -name '*.py' -o -name '*.conf' \) -print0 \
    | xargs -0 sed -i "s|__REPO__|$REPO|g"
}

extract_error_class() {
  local logf="$1" ec sqlstate
  ec="$(grep -oE '\[[A-Z][A-Z0-9_.]+\]' "$logf" 2>/dev/null | head -1 | tr -d '[]')"
  sqlstate="$(grep -oE 'SQLSTATE: [0-9A-Z]+' "$logf" 2>/dev/null | head -1 | sed 's/SQLSTATE: //')"
  if [ -n "$ec" ] && [ -n "$sqlstate" ]; then echo "${ec} (SQLSTATE ${sqlstate})"; else echo "$ec"; fi
}
now_s() { "$PYBIN" -c 'import time;print(f"{time.time():.3f}")'; }

gen_dataset_for_seed() {
  local seed="$1" out="$WORK/orders_seed${seed}.ndjson"
  if [ ! -s "$out" ]; then
    log "generating dataset seed=$seed N=$NROWS"
    "$PYBIN" "$REPO/generators/gen_messy_orders.py" --seed "$seed" --N "$NROWS" \
      > "$out" 2> "$WORK/data-profile-seed${seed}.txt"
  fi
  echo "$out"
}

run_sdp_dry_run() {
  local id="$1" dir="$2" seed="$3" trial="$4" vdir t0 t1 wall ec exit_code stage verdict klass
  klass="${KLASS_OF[$id]}"
  vdir="$WORK/seed${seed}_t${trial}_$dir"
  build_variant "$dir" "$vdir"
  t0="$(now_s)"; set +e
  ( cd "$vdir" && SPARK_REMOTE="$REMOTE" "$PYBIN" "$CLI" dry-run \
      --spec "$vdir/spark-pipeline.yml" > "$vdir/dry-run.log" 2>&1 )
  exit_code=$?; set -e
  t1="$(now_s)"; wall="$("$PYBIN" -c "print(f'{$t1-$t0:.3f}')")"
  ec="$(extract_error_class "$vdir/dry-run.log")"
  if [ "$exit_code" -ne 0 ]; then stage="analysis"; verdict="CAUGHT at analysis (structural)";
  elif grep -q "Run is COMPLETED" "$vdir/dry-run.log"; then stage="completed"; verdict="NOT caught at analysis (COMPLETED)";
  else stage="unknown"; verdict="UNKNOWN"; fi
  log "seed=$seed trial=$trial $id sdp_dry_run: exit=$exit_code stage=$stage ec='${ec:-none}' wall=${wall}s"
  emit_row "$seed" "$trial" "$id" "sdp_dry_run" "$stage" "$exit_code" "$ec" "$wall" "-" "$verdict"
}

run_quantify() {
  local id="$1" seed="$2" trial="$3" ds="$4" qkey="${QKEY_OF[$id]:-}"
  [ -z "$qkey" ] && return 0
  local t0 t1 wall rows qj="$WORK/seed${seed}_t${trial}_${id}_quant.json"
  t0="$(now_s)"; set +e
  "$PYBIN" "$QUANTIFY" "$qkey" "$ds" > "$qj" 2> "$qj.log"; local qrc=$?; set -e
  t1="$(now_s)"; wall="$("$PYBIN" -c "print(f'{$t1-$t0:.3f}')")"
  if [ "$qrc" -eq 0 ] && [ -s "$qj" ]; then
    rows="$("$PYBIN" -c "import json;print(json.load(open('$qj'))['rows_affected'])")"
    log "seed=$seed trial=$trial $id rows_affected=$rows"
    local v; if [ "$rows" -gt 0 ]; then v="silent corruption CONFIRMED: $rows rows"; else v="latent (0 rows on this dataset)"; fi
    emit_row "$seed" "$trial" "$id" "batch_materialize" "silent-wrong" "0" "" "$wall" "$rows" "$v"
  else
    log "seed=$seed trial=$trial $id quantify FAILED"
    emit_row "$seed" "$trial" "$id" "batch_materialize" "error" "$qrc" "quantify-failed" "$wall" "-" "did not run"
  fi
}

main() {
  mkdir -p "$WORK"; : > "$OUT"
  log "repo=$REPO pyspark=$SPARK_VERSION"
  log "seeds=[$SEEDS] N=$NROWS defects=[$DEFECTS] arms=[$ARMS] trials=$TRIALS"

  IFS=',' read -ra DEFECT_LIST <<< "$DEFECTS"
  IFS=',' read -ra SEED_LIST <<< "$SEEDS"
  IFS=',' read -ra ARM_LIST <<< "$ARMS"

  # quantify-only arms (plain) need no Connect server; sdp arm does
  local need_server=0
  for a in "${ARM_LIST[@]}"; do [ "$a" = "sdp" ] && need_server=1; done
  [ "$need_server" -eq 1 ] && { start_server || { log "FATAL: no Connect server"; exit 1; }; }

  local seed trial id ds
  for seed in "${SEED_LIST[@]}"; do
    ds="$(gen_dataset_for_seed "$seed")"
    for trial in $(seq 1 "$TRIALS"); do
      for id in "${DEFECT_LIST[@]}"; do
        local dir="${DIR_OF[$id]:-}"
        [ -z "$dir" ] && { log "unknown defect id $id, skipping"; continue; }
        for a in "${ARM_LIST[@]}"; do
          case "$a" in
            sdp)   run_sdp_dry_run "$id" "$dir" "$seed" "$trial";;
            plain) run_quantify "$id" "$seed" "$trial" "$ds";;
            *)     log "unknown arm '$a'";;
          esac
        done
        # always quantify the semantic defects so rows_affected is recorded once per (seed,trial)
        run_quantify "$id" "$seed" "$trial" "$ds"
      done
    done
  done
  log "wrote $OUT"
}

main "$@"
