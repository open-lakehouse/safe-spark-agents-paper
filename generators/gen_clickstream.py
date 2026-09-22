#!/usr/bin/env python3
"""Deterministic e-commerce CLICKSTREAM source for HC-2 session funnel (v3 §7).

clickstream + user CDC (reuse generators/gen_customers_cdc.py for the user dim) ->
30-minute-inactivity sessionization -> funnel rollup + DLQ. One record per line:
  {event_id, user_id, event_type, event_time (ISO), payload}

event_type in view < cart < checkout < purchase (the funnel order). Messiness:
  * late / out-of-order events (some stamped minutes earlier) -> must still land
    in the correct session by event time, not arrival time.
  * a few malformed (truncated JSON) rows -> DLQ, nothing dropped silently.
  * user_ids reference the CDC customer ids (c0001..) so the SCD1 user dim joins.

Ground truth: a new session starts when the gap from the previous event of the
same user exceeds 30 minutes (event-time ordered). The funnel counts distinct
users reaching each stage. Truth + invariants (no event dropped/double-counted,
unique-user counts) live in quantify_hc.py. Deterministic per --seed.
"""
import argparse, json, random, sys, datetime as dt

_ap = argparse.ArgumentParser(description="Deterministic clickstream NDJSON on stdout.")
_ap.add_argument("--seed", type=int, default=42, help="RNG seed (default 42)")
_ap.add_argument("--users", type=int, default=60, help="distinct users (default 60)")
_ap.add_argument("--max-sessions", type=int, default=3, help="max sessions per user")
_args, _ = _ap.parse_known_args()
random.seed(_args.seed)

BASE = dt.datetime(2026, 6, 20, 8, 0, 0)
STAGES = ["view", "cart", "checkout", "purchase"]
counts = {"events": 0, "late": 0, "malformed": 0, "sessions": 0}
lines = []
eid = 0

for u in range(1, _args.users + 1):
    uid = f"c{u:04d}"
    # each user has 1..max_sessions sessions, separated by > 30 min gaps.
    n_sessions = random.randint(1, _args.max_sessions)
    cursor = BASE + dt.timedelta(minutes=random.randint(0, 240))
    for _s in range(n_sessions):
        counts["sessions"] += 1
        # how deep into the funnel this session goes (>=1 view).
        depth = random.randint(1, 4)
        for stage_idx in range(depth):
            eid += 1
            # intra-session gap: 1..20 min (always < 30 so same session).
            cursor = cursor + dt.timedelta(minutes=random.randint(1, 20))
            et = cursor
            r = random.random()
            if r < 0.10:                              # late / out-of-order
                et = cursor - dt.timedelta(minutes=random.randint(1, 8))
                counts["late"] += 1
            rec = {"event_id": f"ev{eid:07d}", "user_id": uid,
                   "event_type": STAGES[stage_idx], "event_time": et.isoformat(),
                   "payload": {"page": STAGES[stage_idx]}}
            counts["events"] += 1
            if r >= 0.97:                             # malformed (truncated) -> DLQ
                lines.append(json.dumps(rec)[:-4] + "##")
                counts["malformed"] += 1
            else:
                lines.append(json.dumps(rec))
        # gap to next session: > 30 min so it is a NEW session.
        cursor = cursor + dt.timedelta(minutes=random.randint(35, 120))

random.shuffle(lines)
sys.stderr.write("CLICKSTREAM PROFILE: " + json.dumps(counts) + f"\nTOTAL LINES: {len(lines)}\n")
sys.stdout.write("\n".join(lines) + "\n")
