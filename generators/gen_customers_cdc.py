#!/usr/bin/env python3
"""Deterministic CDC event stream for the SCD pipeline (p2_cdc) and E7.

100 customers, insert + 0-3 updates + ~15% deletes, monotonically increasing `seq`,
shuffled arrival order. Prints the expected SCD oracle to stderr so results can be checked.

  python gen_customers_cdc.py > customers_cdc.ndjson 2> cdc_profile.txt
Oracle (seed=7): total_events=263, distinct=100, current(non-deleted)=87, deleted=13,
history versions=263.
"""
import argparse, json, random, sys, datetime as dt

# --- parameterization (added for the safe_agent_study sweep) ----------------
# Each (task, arm, seed) cell needs its own deterministic CDC stream; every arm
# at a given seed faces byte-identical input. Defaults preserve the original
# behaviour EXACTLY: `--seed 7 --customers 100` reproduces the documented oracle
# (total_events=263, distinct=100, current=87, deleted=13) byte-for-byte, because
# the RNG draw sequence below is unchanged.
_ap = argparse.ArgumentParser(description="Deterministic customer CDC event stream on stdout.")
_ap.add_argument("--seed", type=int, default=7, help="RNG seed (default 7)")
_ap.add_argument("--customers", "--N", dest="customers", type=int, default=100,
                 help="number of distinct customers (default 100)")
_args, _ = _ap.parse_known_args()

random.seed(_args.seed)
BASE = dt.datetime(2026, 6, 20, 0, 0, 0)
TIERS = ["bronze", "silver", "gold", "platinum"]; REGIONS = ["NW", "NE", "SW", "SE", "C"]
seq = 0; events = []; current = {}; versions = {}; deleted = set()

def emit(cid, op, tier, region):
    global seq; seq += 1
    t = BASE + dt.timedelta(minutes=seq)
    events.append({"customer_id": cid, "name": f"Customer {cid[1:]}", "tier": tier,
                   "region": region, "op": op, "seq": seq, "event_time": t.isoformat()})
    versions[cid] = versions.get(cid, 0) + 1

for n in range(1, _args.customers + 1):
    cid = f"c{n:04d}"; tier = random.choice(TIERS); region = random.choice(REGIONS)
    emit(cid, "I", tier, region); current[cid] = (tier, region)
    for _ in range(random.randint(0, 3)):
        tier = random.choice(TIERS); region = random.choice(REGIONS)
        emit(cid, "U", tier, region); current[cid] = (tier, region)
    if random.random() < 0.15:
        # v3: deletes are TOMBSTONES -- op='D' with a NULL payload (tier/region
        # null). A pipeline that keys the current table off the latest *non-null*
        # payload, or that inner-joins away null rows, silently keeps a stale row
        # for a removed customer instead of dropping it (D6). The true current
        # state excludes every tombstoned customer.
        emit(cid, "D", None, None); deleted.add(cid); current.pop(cid, None)

# v3: arrival order is shuffled, so the monotonic `seq` -- not arrival -- is the
# only correct ordering key; a survivor chosen by arrival is arbitrary/wrong.
random.shuffle(events)
exp = {"total_events": len(events), "distinct_customers": _args.customers,
       "current_non_deleted": len(current), "deleted": len(deleted),
       "total_versions_history": sum(versions.values())}
sys.stderr.write("CDC EXPECTED: " + json.dumps(exp) + "\n")
for e in events:
    print(json.dumps(e))
