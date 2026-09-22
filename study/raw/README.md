# Raw experiment data (the complete dump)

The full, unaggregated raw data behind Section 1, included so this repo is self-contained for the
handoff (nothing lives only in a separate working tree). Two forms of the same data:

- `sasa_raw_data_20260628.tar.gz` : the canonical archive (provenance-stamped, redistributable).
- `raw_20260628/` : the same dump unpacked for direct browsing:
  - `all_results.jsonl` : every run row with a `_source` sweep tag.
  - `by_sweep/*.jsonl` : the rows split per sweep.
  - `transcripts.tar.gz` : the per-run agent transcripts (each row's `transcript_path` resolves inside).
  - `MANIFEST.md` : row counts, arms, seeds, and the instrument git SHA per sweep.

## What this is (and is not)
This is the historical **raw sweep data** (the N=3 exploratory sweeps: A2 re-run, B/B1 primary
multiseed, the D3 race-fix, and the A/B/B2 pilot; 316 rows total, see `MANIFEST.md`). It is the
evidence trail behind the pre-registration and deviation record (`../PREREGISTRATION.md`, `../DEVIATIONS.md`).

The paper's **headline numbers come from the powered run**, not these sweeps: see
`../results/`, `../POWERED_REPORT.final.json`, and `../POWERED_HEADLINE.final.md`. These raw sweeps
are kept for provenance, auditability, and reanalysis, not as the reported result.

## Provenance and authenticity
Data authenticity, the pre-registration, and the audit that corrected earlier over-claims are
documented in `../PREREGISTRATION.md` and `../DEVIATIONS.md`. Each sweep row carries its instrument
git SHA (`MANIFEST.md`), so any row can be traced to the exact harness that produced it.
(`raw_20260628/MANIFEST.md` references an older `LAB_NOTEBOOK.md`; that forensic notebook was
consolidated into `../DEVIATIONS.md` and `../PREREGISTRATION.md`.) Secret-scanned before commit (no
credentials, tokens, or account identifiers in the rows or transcripts).
