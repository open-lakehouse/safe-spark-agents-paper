#!/usr/bin/env bash
# D7 attribution test (reproducible, self-locating). Augments the frozen pyspark-sdp skill with the
# UTC column idiom (utc_section.md, alongside), re-runs arm B on the 3 D7-shipping tasks, RESTORES the
# frozen skill (trap, always), then compares D7 ships (baseline arm-B vs tzfix arm-B).
# Baseline Spark 4.1.0.dev4: frozen-B D7 = 7 -> tzfix-B D7 = 0 (skill gap). Re-run on new Spark.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STUDY="$(cd "$HERE/../.." && pwd)"                 # repro/tzfix_d7_test -> experiments/safe_agent_study
SKILL="$STUDY/skills/pyspark-sdp/SKILL.md"
BACKUP="$HERE/pyspark-sdp.SKILL.frozen.bak"
OUT="$STUDY/results/results.tzfix.jsonl"
LOG="$STUDY/tzfix.log"
cd "$STUDY" || exit 1

restore(){ [ -f "$BACKUP" ] && cp "$BACKUP" "$SKILL" && echo "$(date) RESTORED frozen skill" >> "$LOG"; }
trap restore EXIT                                   # restore no matter how we exit

SPARK=$(python3 -c 'import pyspark;print(pyspark.__version__)')
echo "$(date) === TZFIX D7 TEST START (spark $SPARK) ===" >> "$LOG"
cp "$SKILL" "$BACKUP"                               # back up frozen skill
cat "$HERE/utc_section.md" >> "$SKILL"              # augment with the UTC idiom

python3 -m harness.runner --backend local \
  --only-tasks p8_currency_normalize,new_stream_stream_join,p14_fx_settlement --only-arms B \
  --local-connect-port 15045 --local-ui-port 4085 --per-cell-timeout 1800 \
  --out "$OUT" --work-dir "$STUDY/.work.tzfix" >> "$LOG" 2>&1
RC=$?
echo "$(date) === TZFIX runner rc=$RC ===" >> "$LOG"
# restore fires via trap here
python3 "$HERE/compare_d7.py" "$OUT" | tee -a "$LOG"
