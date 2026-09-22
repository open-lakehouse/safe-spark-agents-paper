#!/usr/bin/env python3
"""Daily FX -> USD, the ONE source of truth (corpus v3 §8).

Corpus v2 used a single flat rate per currency, so "the FX rate as-of the
payment's UTC date" (D7) only ever mattered for which *day bucket* a row landed
in, never for the rate value itself. v3 makes the rate **change per calendar
day**, so taking the date in the wrong timezone now also picks the wrong *rate*
-- a strictly harder, more realistic D7. To keep that deterministic and
oracle-checkable, the rate is a PURE function of (currency, UTC date) defined
here and imported verbatim by:

  * generators/gen_payments.py        (stamps the messy stream)
  * generators/gen_fx_rates_cdc.py    (emits the rate-change feed the agent must join)
  * experiments/defect_battery/quantify_ext.py  (pay_d7 / pay_d8 ground truth)
  * harness/output_oracles.py    (live USD reconciliation truth)

There is exactly one copy of the numbers; a test asserts every consumer agrees.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

# base rate -> USD per currency (USD == 1.0). v3 widens the basket with exotic
# codes (AUD/CHF/SEK/INR/BRL) so the "unknown vs foreign" boundary is non-trivial.
BASE_FX = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "JPY": 0.0067,
    "CAD": 0.73,
    "AUD": 0.66,
    "CHF": 1.12,
    "SEK": 0.095,
    "INR": 0.012,
    "BRL": 0.205,
}
FOREIGN = ["EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "SEK", "INR", "BRL"]
BAD_CCY = ["XXX", "ZZZ", "us$", "EURO"]   # unknown / garbage currency codes

# reference epoch for the daily drift table (the payments stream starts here).
_FX_EPOCH = dt.date(2026, 6, 20)
# a fixed, deterministic per-day multiplier (no RNG): index = (date - epoch).days.
# small, exact decimals so amount*rate stays reproducible across Python/Spark.
_DAY_FACTOR = [1.00, 1.01, 0.99, 1.02, 0.98, 1.015, 0.985]


def _day_factor(d: dt.date) -> float:
    return _DAY_FACTOR[(d - _FX_EPOCH).days % len(_DAY_FACTOR)]


def fx_usd(currency: str, on_date: dt.date) -> Optional[float]:
    """USD value of 1 unit of `currency` on UTC calendar day `on_date`.

    Returns None for an unknown/garbage currency (the row must be quarantined,
    not silently summed). Deterministic and exact for a given (currency, date).
    """
    base = BASE_FX.get(currency)
    if base is None:
        return None
    return round(base * _day_factor(on_date), 6)


def rate_table(dates):
    """All (currency, date) -> rate pairs over the given iterable of dates, for
    the rate-change feed and for building the ground-truth join in the oracle."""
    out = {}
    for d in dates:
        for ccy in BASE_FX:
            out[(ccy, d)] = fx_usd(ccy, d)
    return out


if __name__ == "__main__":
    import json
    days = [_FX_EPOCH + dt.timedelta(days=i) for i in range(4)]
    print(json.dumps({f"{c}@{d.isoformat()}": fx_usd(c, d)
                      for d in days for c in BASE_FX}, indent=2))
