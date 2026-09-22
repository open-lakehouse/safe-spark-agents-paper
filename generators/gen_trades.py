#!/usr/bin/env python3
"""Deterministic multi-currency TRADE ledger source for HC-1 (corpus v3 §7).

bronze `trades` -> as-of join to the FX-rate feed -> USD valuation -> per-currency
position MERGE. One record per line:
  {trade_id, account_id, currency, notional, event_time (ISO), side}

Messiness (all deterministically gradable):
  * notional as a quoted string on some rows (type coercion; feeds D8/D2).
  * a few rows near a UTC day boundary stamped with a -08:00 local offset, so the
    as-of FX day is wrong if the date is taken in session-local tz (D7).
  * a small fraction of unknown/garbage currency codes that must be quarantined.

The correct USD value of a trade is notional * fx_usd(currency, UTC date of
event_time); the per-currency position is the sum over its trades. Ground truth +
the cross-stage reconciliation invariants live in quantify_hc.py. Deterministic
per --seed.
"""
import argparse, json, os, random, sys, datetime as dt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fx as _fx

_ap = argparse.ArgumentParser(description="Deterministic trades NDJSON on stdout.")
_ap.add_argument("--seed", type=int, default=42, help="RNG seed (default 42)")
_ap.add_argument("--N", type=int, default=1200, help="record count (default 1200)")
_args, _ = _ap.parse_known_args()
random.seed(_args.seed)

BASE = dt.datetime(2026, 6, 20, 0, 0, 0)
CCY = _fx.FOREIGN + ["USD"]
BAD = _fx.BAD_CCY
SIDES = ["BUY", "SELL"]
counts = {"clean": 0, "notional_str": 0, "tz_offset": 0, "bad_ccy": 0}
lines = []

for i in range(_args.N):
    tid = f"t{i:07d}"
    acct = f"a{random.randint(1, 200):04d}"
    # spread across ~4 UTC days so the per-day FX rate matters.
    t = BASE + dt.timedelta(seconds=i * 280)
    ccy = random.choice(CCY)
    notional = round(random.uniform(100, 50000), 2)
    rec = {"trade_id": tid, "account_id": acct, "currency": ccy,
           "notional": notional, "event_time": t.isoformat(),
           "side": random.choice(SIDES)}
    r = random.random()
    if r < 0.05:
        rec["currency"] = random.choice(BAD); counts["bad_ccy"] += 1
    elif r < 0.13:
        rec["notional"] = f"{notional:.2f}"; counts["notional_str"] += 1
    elif r < 0.20:
        local_t = BASE + dt.timedelta(days=(i % 4), hours=2, minutes=(i % 50))
        rec["event_time"] = local_t.isoformat() + "-08:00"; counts["tz_offset"] += 1
    else:
        counts["clean"] += 1
    lines.append(json.dumps(rec))

random.shuffle(lines)
sys.stderr.write("TRADES PROFILE: " + json.dumps(counts) + f"\nTOTAL LINES: {len(lines)}\n")
sys.stdout.write("\n".join(lines) + "\n")
