#!/usr/bin/env python3
"""Deterministic email-subject stream for the UDF classifier task (corpus v3 §6).

One record per line: {email_id, subject}. The subject must be classified into
urgent / spam / routing / info. The corpus plants two SILENT, deterministically
gradable footguns that a naive UDF gets wrong while reaching COMPLETED:

  1. NULL / empty subject  -> the correct label is `routing` (a human must triage
     it), but a naive classifier maps the null branch to `spam` (or lets the UDF
     throw and defaults the row to spam/info).
  2. NON-ASCII keyword     -> a subject whose only urgency marker is non-Latin
     (e.g. JP/AR/ZH for "urgent") is `urgent`, but an ASCII-only keyword match
     misses it and falls through to `info`.

The ground-truth classifier and the misclassification quantifier live in
experiments/defect_battery/quantify_udf.py and import nothing from here; this
generator only needs to AGREE with that label function, which the unit test
asserts. Deterministic per --seed.
"""
import argparse, json, random, sys

_ap = argparse.ArgumentParser(description="Deterministic email-subject NDJSON on stdout.")
_ap.add_argument("--seed", type=int, default=42, help="RNG seed (default 42)")
_ap.add_argument("--N", type=int, default=900, help="record count (default 900)")
_args, _ = _ap.parse_known_args()
random.seed(_args.seed)

# pools chosen so each maps UNAMBIGUOUSLY to one true category (see quantify_udf).
URGENT = ["URGENT: prod is down", "Please respond ASAP", "CRITICAL alert: disk full",
          "Emergency: pager firing", "Action needed !!! now"]
SPAM = ["You are a WINNER, claim your prize", "FREE offer just for you",
        "Huge discount sale today", "Cheap meds online", "Congratulations winner"]
ROUTING = ["RE: ticket 4821", "FWD: invoice 0093", "order # 55120 question",
           "Re: your support ticket", "Fwd: contract"]
INFO = ["Weekly newsletter", "Team lunch notes", "Q3 roadmap summary",
        "Office closed Monday", "Release notes 4.1"]
# non-ASCII urgency markers (no Latin urgency word) -> correct=urgent, naive=info.
NONASCII_URGENT = ["緊急: サーバー停止", "ضروري: تعطل الخادم", "紧急：服务器宕机"]

counts = {"normal": 0, "null_subject": 0, "empty_subject": 0, "nonascii_urgent": 0}
lines = []
for i in range(_args.N):
    eid = f"e{i:06d}"
    r = random.random()
    if r < 0.08:
        subj = None; counts["null_subject"] += 1
    elif r < 0.13:
        subj = "   "; counts["empty_subject"] += 1
    elif r < 0.23:
        subj = random.choice(NONASCII_URGENT); counts["nonascii_urgent"] += 1
    else:
        subj = random.choice(random.choice([URGENT, SPAM, ROUTING, INFO]))
        counts["normal"] += 1
    rec = {"email_id": eid, "subject": subj}   # subj may be None -> JSON null
    lines.append(json.dumps(rec, ensure_ascii=False))

random.shuffle(lines)
sys.stderr.write("EMAILS PROFILE: " + json.dumps(counts, ensure_ascii=False)
                 + f"\nTOTAL LINES: {len(lines)}\n")
sys.stdout.write("\n".join(lines) + "\n")
