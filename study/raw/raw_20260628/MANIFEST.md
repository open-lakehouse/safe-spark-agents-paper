# Safe-Spark-Agents — raw data dump (2026-06-28)

Consolidated kept experiment data. `all_results.jsonl` is every row with a `_source` tag;
`by_sweep/` has them split per sweep; `transcripts.tar.gz` holds the per-run agent transcripts
(paths preserved; each row's `transcript_path` resolves inside it).

**Total rows: 316** (A+B2 Decision-B sweep is still running — its rows append on completion).

| sweep (_source) | rows | arms | seeds | git_sha |
|---|---:|---|---|---|
| A2_rerun_instr_v3.1 | 66 | A2 | [42, 1337, 2718] | 1d28563 |
| AB2_decisionB_inprogress | 0 |  | [] |  |
| B_B1_primary_seed42 | 66 | A2,B,B1 | [42] | 295d725 |
| B_B1_primary_multiseed | 132 | A2,B,B1 | [1337, 2718] | 54834a1 |
| A2_D3_racefix | 13 | A2 | [1337, 2718] | 9cc6342 |
| pilot_A_B_B2_seed42 | 39 | A,B,B2 | [42] | c539359 |

Provenance & data authenticity: see `../../LAB_NOTEBOOK.md`. Instruments: A2 re-run on
`instrument-v3.1` (1d28563); B/B1 on 295d725/54834a1; D3 on 9cc6342; pilot single-seed.
