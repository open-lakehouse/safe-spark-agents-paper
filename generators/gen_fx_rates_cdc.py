#!/usr/bin/env python3
"""Deterministic FX-rate CHANGE feed (corpus v3 §5/§7).

A CDC-style stream of rate changes that the agent must join AS-OF event time:
one event per (currency, UTC day) carrying the rate effective from that day, plus
a few REVISIONS (a wrong rate superseded by a corrected one at a higher `seq`) so
that ordering by `seq` -- not by arrival -- is the only correct survivor rule.
Arrival order is shuffled.

Schema (one Kafka `value` per line):
  currency, rate (USD per 1 unit), effective_date (YYYY-MM-DD),
  effective_time (ISO, UTC midnight of effective_date), op ('U'), seq

The "correct" rate for (currency, date) is generators/fx.fx_usd(currency, date); the
revisions deliberately emit a wrong value FIRST so a survivor chosen without
ORDER BY seq is wrong. Used by: new_stream_stream_join, new_scd2_as_of_join,
HC-1 fx_trade_ledger. Deterministic per --seed.
"""
import argparse, json, os, random, sys, datetime as dt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fx as _fx

_ap = argparse.ArgumentParser(description="Deterministic FX-rate change feed on stdout.")
_ap.add_argument("--seed", type=int, default=42, help="RNG seed (default 42)")
_ap.add_argument("--days", type=int, default=5, help="number of UTC days of rates (default 5)")
_args, _ = _ap.parse_known_args()
random.seed(_args.seed)

dates = [_fx._FX_EPOCH + dt.timedelta(days=i) for i in range(_args.days)]
seq = 0
events = []
revisions = 0


def emit(ccy, rate, date, op="U"):
    global seq
    seq += 1
    t = dt.datetime(date.year, date.month, date.day)
    events.append({"currency": ccy, "rate": rate, "effective_date": date.isoformat(),
                   "effective_time": t.isoformat(), "op": op, "seq": seq})


for d in dates:
    for ccy in _fx.BASE_FX:
        correct = _fx.fx_usd(ccy, d)
        # ~12% of (ccy,day) cells get a WRONG rate first, then a corrected revision
        # at a higher seq. Order-by-seq -> correct; arbitrary survivor -> wrong.
        if ccy != "USD" and random.random() < 0.12:
            emit(ccy, round(correct * 1.5, 6), d)      # wrong (superseded)
            emit(ccy, correct, d)                       # correction (latest seq wins)
            revisions += 1
        else:
            emit(ccy, correct, d)

random.shuffle(events)
sys.stderr.write("FX RATES FEED: " + json.dumps(
    {"days": _args.days, "currencies": len(_fx.BASE_FX), "events": len(events),
     "revisions": revisions}) + f"\nTOTAL LINES: {len(events)}\n")
for e in events:
    print(json.dumps(e))
