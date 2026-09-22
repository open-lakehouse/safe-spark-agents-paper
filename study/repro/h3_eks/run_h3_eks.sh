#!/usr/bin/env bash
# =============================================================================
# run_h3_eks.sh — H3 (data-processing compute / executor-seconds) on EKS, Phase 2b
# =============================================================================
# PLAN / SKELETON ONLY. THIS DOES NOT RUN A CONFIRMATORY H3 STUDY YET.
# H3 has NOT been run; there are ZERO A-vs-B executor-seconds results.
# This script encodes the run *shape* from REPRODUCE-H3.md and is INERT until:
#   (1) every prerequisite in REPRODUCE-H3.md §1 is green (esp. P2 per-attempt
#       serialization at runner.py:258-260, and P3 remote Arm-B SDP green), AND
#   (2) the operator fills the NEEDS placeholders below (EKS endpoint + $ ceiling)
#       and removes the RUN_GUARD.
#
# Place under: experiments/safe_agent_study/repro/h3_eks/run_h3_eks.sh
# Measurement mechanism: D-5 before/after Spark stage-diff (live.py:837-897).
# Metric selection: analyze.py --assume-backend live -> measured
#   executor_seconds_to_correct (cross-arm comparable ONLY on the uniform
#   live/Connect substrate).
# =============================================================================
set -euo pipefail

# ----- RUN GUARD -------------------------------------------------------------
# Refuse to launch until an operator has reviewed prereqs + set the ceiling.
# Remove this line ONLY after REPRODUCE-H3.md §1 P1-P8 are all green.
: "${I_HAVE_CHECKED_H3_PREREQS:?Refusing to run: read REPRODUCE-H3.md §1, then set I_HAVE_CHECKED_H3_PREREQS=1}"

# ----- NEEDS: EKS endpoint (P8) ---------------------------------------------
# The runner reads spark_remote / spark_rest_url from study.config.json for
# --backend live (NOT from CLI). Set them there, OR export overrides here and
# have your wrapper patch study.config.json before invoking the runner.
#
#   spark_remote   = mTLS Connect endpoint reached via the egress sidecar /
#                    local mTLS proxy, e.g. sc://127.0.0.1:15009  (raw :15002
#                    is loopback-only and never exposed; RUNBOOK §1/§5)
#   spark_rest_url = Connect DRIVER UI REST base (P7); MUST be reachable or the
#                    stage-diff degrades to (None,None) + wall-clock cross-check
SPARK_REMOTE="${SPARK_REMOTE:-NEEDS_EKS_CONNECT_ENDPOINT}"      # e.g. sc://127.0.0.1:15009
SPARK_REST_URL="${SPARK_REST_URL:-NEEDS_EKS_DRIVER_REST_URL}"  # e.g. http://127.0.0.1:4040

# ----- NEEDS: $ ceiling (P6, the "spend gate") ------------------------------
# No dollar spend-cap exists in code; N is bounded operationally. Set the ceiling
# and the subset so cells * per_cell_usd_est <= USD_CEILING (see REPRODUCE-H3.md §5).
USD_CEILING="${USD_CEILING:-NEEDS_DOLLAR_CEILING}"             # e.g. 25.00

# ----- required credentials --------------------------------------------------
: "${ANTHROPIC_API_KEY:?live backend requires ANTHROPIC_API_KEY (runner.py:1179)}"

# ----- scope (edit for the cost-bounded subset) ------------------------------
SMOKE_TASKS="${SMOKE_TASKS:-NEEDS_TASK_A,NEEDS_TASK_B,NEEDS_TASK_C}"   # 2-3 tasks
RUN_TASKS="${RUN_TASKS:-$SMOKE_TASKS}"                                  # cost-bounded subset
RUN_MAX_SEEDS="${RUN_MAX_SEEDS:-1}"                                     # bump for ~40-cell run
PER_CELL_TIMEOUT="${PER_CELL_TIMEOUT:-1800}"

# -----------------------------------------------------------------------------
STUDY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"   # -> experiments/safe_agent_study
OUT_DIR="$STUDY_DIR/repro/h3_eks"
mkdir -p "$OUT_DIR"
cd "$STUDY_DIR"

echo "== H3/EKS Phase-2b (PLAN) =="
echo "  study dir     : $STUDY_DIR"
echo "  spark_remote  : $SPARK_REMOTE   (set in study.config.json for --backend live)"
echo "  spark_rest_url: $SPARK_REST_URL (driver UI REST; MUST be reachable, P7)"
echo "  \$ ceiling     : $USD_CEILING"
echo

# Fail fast if placeholders were left unfilled.
for v in "$SPARK_REMOTE" "$SPARK_REST_URL" "$USD_CEILING" "$SMOKE_TASKS"; do
  case "$v" in NEEDS_*|*NEEDS_*) echo "ERROR: unfilled NEEDS placeholder: $v"; exit 2;; esac
done

# =============================================================================
# STEP 1 — SMOKE (2-3 tasks x 1 seed x {A,B}) — proves the substrate, NOT a result
# =============================================================================
smoke() {
  echo "== STEP 1: SMOKE =="
  python3 -m harness.runner \
    --backend live \
    --only-arms A,B \
    --only-tasks "$SMOKE_TASKS" \
    --max-seeds 1 \
    --per-cell-timeout "$PER_CELL_TIMEOUT" \
    --out "$OUT_DIR/results.h3_smoke.jsonl" \
    --work-dir "$OUT_DIR/.work.h3_smoke"
  echo "SMOKE done. VERIFY BEFORE PROCEEDING:"
  echo "  - Arm A AND Arm B each 'completes green' (Run is COMPLETED + table read-back)"
  echo "  - each executed cell has NON-NULL executor_seconds (stage-diff fired, not the None fallback)"
  echo "  - use the smoke's measured executor_seconds + tokens to size N (REPRODUCE-H3.md §5)"
}

# =============================================================================
# STEP 2 — COST-BOUNDED CONFIRMATORY SWEEP (~40 cells, behind the $ ceiling)
# =============================================================================
run_sweep() {
  echo "== STEP 2: COST-BOUNDED SWEEP =="
  echo "  Confirm cells x per_cell_usd_est <= $USD_CEILING (from smoke calibration) BEFORE this."
  python3 -m harness.runner \
    --backend live \
    --only-arms A,B \
    --only-tasks "$RUN_TASKS" \
    --max-seeds "$RUN_MAX_SEEDS" \
    --per-cell-timeout "$PER_CELL_TIMEOUT" \
    --out "$OUT_DIR/results.h3_eks.jsonl" \
    --work-dir "$OUT_DIR/.work.h3_eks"
}

# =============================================================================
# STEP 3 — ANALYZE -> H3.1 / H3.2 (measured stage-diff via --assume-backend live)
# =============================================================================
analyze() {
  echo "== STEP 3: ANALYZE =="
  SPARK_HOME="$(python3 -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')" \
  python3 analysis/analyze.py "$OUT_DIR/results.h3_eks.jsonl" \
    --assume-backend live \
    --tasks config/TASKS.lock.json \
    --md-out "$OUT_DIR/HEADLINE.h3_eks.md" \
    --json-out "$OUT_DIR/REPORT.h3_eks.json"
  echo "H3.1 requires the P2 per-attempt fields in per_iteration (runner.py:258-260)."
  echo "H3.2 = measured executor_seconds_to_correct A-vs-B; report jointly with H5.3."
}

case "${1:-smoke}" in
  smoke)   smoke ;;
  sweep)   run_sweep ;;
  analyze) analyze ;;
  all)     smoke; echo "--- pause: verify smoke + size N, then re-run with 'sweep' ---" ;;
  *) echo "usage: $0 {smoke|sweep|analyze|all}"; exit 1 ;;
esac
