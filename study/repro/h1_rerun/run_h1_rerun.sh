#!/usr/bin/env bash
# H1 rerun (fair-skill): full arm-B study with the UTC-augmented pyspark-sdp skill, to MEASURE (not
# infer) whether the silent-defect residue converges to A once B has the paradigm-appropriate idiom.
# Arm A is UNCHANGED (bare imperative loads no skill) -> reuse frozen A from the primary run.
# Instrument = instrument-v3.2-frozen + repro/tzfix_d7_test/utc_section.md appended (documented variant).
# Augment -> run B (22 tasks x 12 seeds) -> RESTORE frozen skill (trap) -> merge frozen-A + rerun-B -> analyze + compare.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STUDY="$(cd "$HERE/../.." && pwd)"
SKILL="$STUDY/skills/pyspark-sdp/SKILL.md"
UTC="$STUDY/repro/tzfix_d7_test/utc_section.md"
BACKUP="$HERE/pyspark-sdp.SKILL.frozen.bak"
OUT="$STUDY/results/results.h1rerun.B.jsonl"
LOG="$STUDY/h1rerun.log"
cd "$STUDY" || exit 1
restore(){ [ -f "$BACKUP" ] && cp "$BACKUP" "$SKILL" && echo "$(date) RESTORED frozen skill" >> "$LOG"; }
trap restore EXIT

SPARK=$(python3 -c 'import pyspark;print(pyspark.__version__)')
echo "$(date) === H1 RERUN START (arm B, fixed skill, spark $SPARK) ===" >> "$LOG"
cp "$SKILL" "$BACKUP"
cat "$UTC" >> "$SKILL"

python3 -m harness.runner --backend local --only-arms B --max-seeds 12 \
  --local-connect-port 15046 --local-ui-port 4086 --per-cell-timeout 1800 \
  --out "$OUT" --work-dir "$STUDY/.work.h1rerun" >> "$LOG" 2>&1
RC=$?
echo "$(date) === H1 RERUN runner rc=$RC ===" >> "$LOG"
# skill restored via trap on exit; if breaker tripped (rc!=0), backfill the missing B cells then re-run merge.
if [ "$RC" -eq 0 ]; then
  python3 "$HERE/merge_compare.py" | tee -a "$LOG"
else
  echo "$(date) rc=$RC (breaker/timeouts?) — backfill missing B cells (repro/backfill), then: python3 $HERE/merge_compare.py" >> "$LOG"
fi
