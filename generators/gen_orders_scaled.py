#!/usr/bin/env python3
"""Parameterized messy-orders generator for the scale ladder (E2/E5).

Streams NDJSON to stdout, same deliberate messy-data profile as gen_messy_orders.py,
but with a caller-chosen row count so the same shape can be tested at 1e4 .. 1e9.
Deterministic per (N, seed) so runs reproduce. Memory-safe: streams, never buffers.

Usage:
  python gen_orders_scaled.py <N> [seed]            > orders_<N>.ndjson
  python gen_orders_scaled.py 100000000 42 | kafka-console-producer.sh ... --topic orders
"""
import json, random, sys, datetime as dt

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
SEED = int(sys.argv[2]) if len(sys.argv) > 2 else 42
random.seed(SEED)
BASE = dt.datetime(2026, 6, 20)
M = [f"m{n:03d}" for n in range(1, 21)]
CATS = ["grocery", "electronics", "apparel", "fuel", "dining"]
out = sys.stdout.write

# keep the failure-mode fractions identical to the laptop dataset so the messy
# profile is constant across scales (only volume changes)
for i in range(N):
    t = BASE + dt.timedelta(seconds=(i % 120000) * 3)
    rec = {"order_id": f"o{i:010d}", "merchant_id": random.choice(M),
           "event_time": t.isoformat(), "amount": round(random.uniform(2, 400), 2),
           "category": random.choice(CATS)}
    r = random.random()
    if r < 0.06:                      # exact duplicate (emit twice)
        out(json.dumps(rec) + "\n")
    elif r < 0.12: rec["event_time"] = (t - dt.timedelta(hours=3)).isoformat()  # late
    elif r < 0.17: rec["merchant_id"] = None                                    # null key
    elif r < 0.22: rec.pop("category")                                          # missing field
    elif r < 0.27: rec["amount"] = f"{rec['amount']:.2f}"                        # string amount
    elif r < 0.32: rec["event_time"] = int(t.timestamp() * 1000)                # epoch-ms
    elif r < 0.40: rec["merchant_id"] = f"x{random.randint(900,999)}"           # unknown key
    out(json.dumps(rec) + "\n")
    if 0.40 <= r < 0.43:                                                        # malformed tail
        out(json.dumps(rec)[:-3] + ",,,\n")
