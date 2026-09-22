#!/usr/bin/env python3
"""Generate a deterministic multi-currency PAYMENTS stream as NDJSON on stdout.

A second, independent semantic substrate for the safe-agent study, so the
timezone (D7) and silent-drop (D8) defect classes are measured on data with a
different schema and a different failure mechanism than the orders stream -- the
point being that per-class detection rates are NOT single-dataset artifacts.

Schema (one Kafka `value` per line):
  payment_id, account_id, event_time (ISO, with a +TZ offset on some rows),
  currency (USD and several foreign codes; some BAD/unknown codes),
  amount_minor (integer minor units, e.g. cents), amount (major-unit string on
  some rows -> the string-vs-number trap), settled (bool)

Deliberate messiness (catalogued on stderr):
  1. foreign currency        -> a naive `sum(amount) where currency='USD'` or an
                                un-normalized sum silently DROPS/​mis-totals the
                                non-USD value (D8).
  2. tz-offset event_time    -> the FX rate "as of the event DATE" is wrong if the
                                date is taken in session-local tz instead of UTC,
                                so the converted USD amount lands on the wrong FX
                                day (D7).
  3. amount as string        -> type-coercion trap (feeds D8 under a numeric schema).
  4. unknown/bad currency     -> must be quarantined, else its rows corrupt the sum.
  5. null amount             -> dropped silently by SUM if unguarded.

Deterministic per --seed; defaults (--seed 42 --N 4000) define the locked oracle
numbers used by the unit tests. FX rates are FIXED (not random) so the USD
conversion is a pure function of (currency, UTC date).
"""
import argparse, json, os, random, sys, datetime as dt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fx as _fx   # the ONE daily-FX source of truth (generators/fx.py)

_ap = argparse.ArgumentParser(description="Deterministic multi-currency payments NDJSON on stdout.")
_ap.add_argument("--seed", type=int, default=42, help="RNG seed (default 42)")
_ap.add_argument("--N", type=int, default=4000, help="base record count (default 4000)")
_args, _ = _ap.parse_known_args()

random.seed(_args.seed)
N = _args.N
BASE = dt.datetime(2026, 6, 20, 0, 0, 0)         # UTC midnight reference

# v3: FX changes per UTC day (generators/fx.py) and the foreign basket is widened with
# exotic codes (AUD/CHF/SEK/INR/BRL), so the as-of-DATE rate -- not just the day
# bucket -- now matters. USD conversion is a pure function of (currency, UTC date);
# the generator only stamps currency+amount, the agent must look the rate up.
FOREIGN = _fx.FOREIGN
BAD_CCY = _fx.BAD_CCY

counts = {k: 0 for k in (
    "clean_usd", "foreign", "tz_offset", "amount_str", "bad_ccy", "null_amount")}
lines = []


def iso(t):
    return t.isoformat()


for i in range(N):
    pid = f"p{i:07d}"
    acct = f"a{random.randint(1, 500):04d}"
    # spread across ~4 UTC days so the per-day FX rate genuinely varies; a slice
    # sits within a few hours of UTC midnight so a tz offset can push it across a
    # day boundary (and therefore onto the wrong day's rate).
    t = BASE + dt.timedelta(seconds=i * 86)
    ccy = "USD"
    amount_minor = random.randint(100, 80000)     # 1.00 .. 800.00 in minor units
    rec = {
        "payment_id": pid,
        "account_id": acct,
        "event_time": iso(t),
        "currency": ccy,
        "amount_minor": amount_minor,
        "amount": round(amount_minor / 100.0, 2),
        "settled": True,
    }
    r = random.random()
    if r < 0.28:                                   # 1. foreign currency
        rec["currency"] = random.choice(FOREIGN); counts["foreign"] += 1
    elif r < 0.34:                                 # 4. bad/unknown currency code
        rec["currency"] = random.choice(BAD_CCY); counts["bad_ccy"] += 1
    else:
        counts["clean_usd"] += 1

    if 0.34 <= r < 0.42:                           # 2. tz-offset near a day boundary
        # place the instant in the early-UTC-morning window and stamp a -08:00
        # local offset, so session-local date != UTC date for these rows.
        local_t = BASE + dt.timedelta(days=(i % 4), hours=2, minutes=(i % 50))
        rec["event_time"] = iso(local_t) + "-08:00"
        counts["tz_offset"] += 1
    if 0.42 <= r < 0.50:                            # 3. amount as a major-unit string
        rec["amount"] = f"{rec['amount_minor'] / 100.0:.2f}"
        counts["amount_str"] += 1
    if 0.50 <= r < 0.535:                           # 5. null amount
        rec["amount"] = None; rec["amount_minor"] = None
        counts["null_amount"] += 1

    lines.append(json.dumps(rec))

random.shuffle(lines)
sys.stderr.write("PAYMENTS PROFILE: " + json.dumps(counts) + f"\nTOTAL LINES: {len(lines)}\n")
sys.stdout.write("\n".join(lines) + "\n")
