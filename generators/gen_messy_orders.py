#!/usr/bin/env python3
"""Generate genuinely-messy order events as NDJSON on stdout.

Piped into Kafka's console producer. Deterministic (seeded) so the two agent
sandboxes face the *identical* data and the failure modes are reproducible.

Each line is one Kafka record value. The messiness is deliberate and catalogued
in MESSY_README (printed to stderr) so we can score which traps each agent hits:

  1. duplicate order_id            -> needs dedup; unbounded state w/o watermark
  2. late / out-of-order event_time-> needs watermark; silently wrong otherwise
  3. null / missing merchant_id    -> join key blows up or drops rows
  4. schema drift (missing fields) -> NPE / wrong agg if unguarded
  5. amount as string vs number    -> type coercion bug
  6. mixed timestamp formats       -> parse to null -> dropped silently
  7. malformed JSON (not parseable)-> corrupt record; must be quarantined
  8. unknown merchant_id           -> left-join null; inner-join silent data loss
"""
import argparse, json, random, sys, datetime as dt

# --- parameterization (added for the safe_agent_study sweep) ----------------
# The study runs the SAME messy-data generator across many fixed seeds (one per
# matched (task, arm, seed) cell) so every arm faces byte-identical input per
# seed. Defaults preserve the original behaviour EXACTLY: `--seed 42 --N 5000`
# reproduces the pre-existing E3 dataset (and its oracle numbers D2=246, D7=275,
# D8=250/$49,778.06) byte-for-byte, because the sequence of RNG draws below is
# unchanged. Only the seed value and row count are now injectable.
_ap = argparse.ArgumentParser(description="Generate deterministic messy orders NDJSON on stdout.")
_ap.add_argument("--seed", type=int, default=42, help="RNG seed (default 42; identical stream per seed)")
_ap.add_argument("--N", type=int, default=5000, help="base record count before dup/malformed injection (default 5000)")
_ap.add_argument("--v3", action="store_true",
                 help="append corpus-v3 realism rows (nested category struct, line_items "
                      "array-of-structs amount drift, 502-HTML junk) AFTER the base stream. "
                      "Default OFF preserves the v2 reference numbers (D2=246/D7=275/D8=250) "
                      "byte-for-byte; the appended rows use an INDEPENDENT derived RNG and are "
                      "designed to leave D2/D6/D7/D8 opportunity counts unchanged while adding "
                      "the nested-revenue (D8-nested) and schema-drift dimensions.")
_args, _ = _ap.parse_known_args()

random.seed(_args.seed)               # identical stream for every run at this seed
N = _args.N
BASE = dt.datetime(2026, 6, 20, 0, 0, 0)
KNOWN_MERCHANTS = [f"m{n:03d}" for n in range(1, 21)]   # m001..m020 exist in dim
CATEGORIES = ["grocery", "electronics", "apparel", "fuel", "dining"]

counts = {k: 0 for k in (
    "clean", "dup", "late", "null_mid", "missing_field",
    "amount_str", "ts_epoch", "ts_tz", "malformed", "unknown_mid")}

lines = []

def iso(t):           return t.isoformat()
def epoch_ms(t):      return int(t.timestamp() * 1000)

for i in range(N):
    oid = f"o{i:06d}"
    t = BASE + dt.timedelta(seconds=i * 7)             # ~roughly increasing
    mid = random.choice(KNOWN_MERCHANTS)
    rec = {
        "order_id": oid,
        "merchant_id": mid,
        "event_time": iso(t),
        "amount": round(random.uniform(2, 400), 2),
        "category": random.choice(CATEGORIES),
    }
    r = random.random()
    # inject failure modes on a controlled fraction of records
    if r < 0.06:                                        # 1. duplicate
        lines.append(json.dumps(rec)); counts["clean"] += 1
        lines.append(json.dumps(rec)); counts["dup"] += 1; continue
    elif r < 0.12:                                      # 2. late event (3h back)
        rec["event_time"] = iso(t - dt.timedelta(hours=3)); counts["late"] += 1
    elif r < 0.17:                                      # 3. null merchant_id
        rec["merchant_id"] = None; counts["null_mid"] += 1
    elif r < 0.22:                                      # 4. missing category field
        rec.pop("category"); counts["missing_field"] += 1
    elif r < 0.27:                                      # 5. amount as string
        rec["amount"] = f"{rec['amount']:.2f}"; counts["amount_str"] += 1
    elif r < 0.32:                                      # 6. epoch-millis timestamp
        rec["event_time"] = epoch_ms(t); counts["ts_epoch"] += 1
    elif r < 0.36:                                      # 6b. tz-suffixed timestamp
        rec["event_time"] = iso(t) + "+05:30"; counts["ts_tz"] += 1
    elif r < 0.40:                                      # 8. unknown merchant
        rec["merchant_id"] = f"x{random.randint(900,999)}"; counts["unknown_mid"] += 1
    else:
        counts["clean"] += 1

    if 0.40 <= r < 0.43:                                # 7. malformed JSON line
        lines.append(json.dumps(rec)[:-3] + ",,,")      # truncated/garbage tail
        counts["malformed"] += 1
    else:
        lines.append(json.dumps(rec))

# --- corpus-v3 realism append (deterministic, INDEPENDENT RNG) ---------------
# These rows are added AFTER the base loop using a derived RNG so the base stream
# (and its locked D2/D6/D7/D8 numbers) is byte-for-byte identical to v2. They are
# constructed to be NEUTRAL to the four orders quantifiers:
#   * unique order_ids (no new duplicate keys -> D6 unchanged)
#   * valid ISO event_time at MIDDAY (no epoch / no tz offset -> D2/D7 unchanged)
#   * no scalar string/null `amount` (-> D8 scalar count unchanged)
# while adding the v3 dimensions the new tasks/quantifiers measure:
#   * line_items: amount arrives as an array-of-structs; true revenue = sum(qty*price)
#     and a scalar-only sum silently drops it (D8-nested).
#   * nested category: category as a {name,dept} struct (schema-drift tolerance).
#   * junk: a raw "502 Bad Gateway" HTML body (non-JSON) that must be quarantined.
counts.update({"v3_line_items": 0, "v3_nested_cat": 0, "v3_junk": 0})
if _args.v3:
    r3 = random.Random(_args.seed ^ 0x0C0FFEE)
    M = max(1, N // 12)
    for j in range(M):
        oid = f"v3{j:06d}"
        t = BASE + dt.timedelta(days=r3.randint(0, 3), hours=12, minutes=r3.randint(0, 59))
        mid = r3.choice(KNOWN_MERCHANTS)
        kind = r3.random()
        if kind < 0.45:                                 # line_items array-of-structs
            n_li = r3.randint(1, 4)
            items = [{"sku": f"s{r3.randint(1,99):02d}",
                      "qty": r3.randint(1, 5),
                      "price": round(r3.uniform(2, 120), 2)} for _ in range(n_li)]
            rec = {"order_id": oid, "merchant_id": mid, "event_time": iso(t),
                   "line_items": items, "category": r3.choice(CATEGORIES)}
            lines.append(json.dumps(rec)); counts["v3_line_items"] += 1
        elif kind < 0.80:                               # nested category struct
            rec = {"order_id": oid, "merchant_id": mid, "event_time": iso(t),
                   "amount": round(r3.uniform(2, 400), 2),
                   "category": {"name": r3.choice(CATEGORIES),
                                "dept": r3.choice(["north", "south", "online"])}}
            lines.append(json.dumps(rec)); counts["v3_nested_cat"] += 1
        else:                                           # unstructured junk row
            lines.append("502 Bad Gateway <html><head><title>502</title></head>"
                         "<body><h1>Bad Gateway</h1></body></html>")
            counts["v3_junk"] += 1

random.shuffle(lines)                                   # arrival order != event order
sys.stderr.write("MESSY DATA PROFILE: " + json.dumps(counts, indent=0) + f"\nTOTAL LINES: {len(lines)}\n")
sys.stdout.write("\n".join(lines) + "\n")
