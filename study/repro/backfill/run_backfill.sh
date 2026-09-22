#!/usr/bin/env bash
# Surgical backfill of specific cells the primary run missed (breaker abort) or timed out.
# Self-locating. Two hazards guarded: (1) runner opens --out in "w"/truncate mode -> each invocation
# writes its OWN file; (2) SDP arm-B `run` full-refresh truncates catalog tables -> fresh, wiped
# work-dir per invocation (fresh in-memory catalog). Breaker intact; timeouts at 1800s.
# EDIT the seed-subset files (seeds_*.json here) and the run() calls below for YOUR gaps.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STUDY="$(cd "$HERE/../.." && pwd)"
cd "$STUDY" || exit 1
LOG="$STUDY/backfill.log"
echo "$(date) === BACKFILL START ===" >> "$LOG"

run() {  # $1=task $2=seedfile $3=port_c $4=port_u $5=workdir $6=outfile
  rm -rf "$STUDY/$5"                                # fresh catalog/warehouse per invocation
  echo "$(date) --> $1 (seeds=$2) -> $6 [fresh $5]" >> "$LOG"
  python3 -m harness.runner --backend local \
    --only-tasks "$1" --only-arms B --seeds "$HERE/$2" \
    --local-connect-port "$3" --local-ui-port "$4" \
    --per-cell-timeout 1800 --out "$STUDY/$6" --work-dir "$STUDY/$5" >> "$LOG" 2>&1
}

# EXAMPLE gaps from the 2026-07-02 baseline run (edit for your own):
run HC2_session_funnel   seeds_hc2b.json    15041 4081 .work.bf.hc2b   results/results.bf_hc2b.jsonl   && \
run new_scd2_as_of_join  seeds_scd2b.json   15042 4082 .work.bf.scd2   results/results.bf_scd2.jsonl   && \
run orders_silver_gold   seeds_ordersb.json 15043 4083 .work.bf.orders results/results.bf_orders.jsonl
RC=$?
echo "$(date) === BACKFILL DONE rc=$RC ===" >> "$LOG"
[ "$RC" -eq 0 ] && echo "now run: python3 $HERE/merge_and_analyze.py" >> "$LOG"
