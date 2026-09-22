# Reproduce

The study is built to reproduce **byte-identically**, including on **future Spark releases**, so the
paradigm-safety result can be re-checked as Spark and SDP evolve. Two levels: (1) recompute the paper's
numbers from committed result files; (2) re-run the agents from scratch. Environment prerequisites are in
[`ENV_SETUP.md`](ENV_SETUP.md).

## 0. Provenance
- **Instrument:** frozen `instrument-v3.2-frozen`; every result row is stamped with `git_sha`,
  `image_digest`, `spark_version`, `base_model_id`, so instrument drift is detectable.
- **Determinism:** input data is a pure function of `(generator, args, seed)`. The 12 seeds are locked in
  [`../study/config/SEEDS.lock.json`](../study/config/SEEDS.lock.json); the 22 tasks in
  [`../study/config/TASKS.lock.json`](../study/config/TASKS.lock.json). Same seed → byte-identical data.

## 1. Recompute the paper's numbers (no LLM, no cluster)
The result files behind every number are committed under [`../study/`](../study/):
- `results.powered.AB.n12.final.jsonl`: the 528-cell powered run (H1/H2/H4/H5)
- `results.tzfix.jsonl`: the D7 skill-swap (7 → 0)

```bash
cd study
python3 analysis/analyze.py results/results.powered.AB.n12.final.jsonl --tasks config/TASKS.lock.json --assume-backend local
```

## 2. Re-run the agents (LLM + Spark)
Full runbook: [`../study/repro/REPRODUCE.md`](../study/repro/REPRODUCE.md). The EKS compute run (H3) has its
own runbook + integration log under [`../study/repro/h3_eks/`](../study/repro/h3_eks/). Requires
`ANTHROPIC_API_KEY` and a reachable Spark Connect endpoint (local or the reference EKS cluster).

## 3. The raw run archive (full transcripts + generated data)
The **raw sweep data and per-run agent transcripts are committed in-repo** at
[`../study/raw/`](../study/raw/) (`sasa_raw_data_20260628.tar.gz` plus the unpacked `raw_20260628/`:
`all_results.jsonl`, `by_sweep/`, `transcripts.tar.gz`, `MANIFEST.md`), so no download is needed to
inspect the raw rows or transcripts; see `study/raw/README.md`.

For the *full* replay archive (additionally every generated input, every materialized output, and
every grade) download the larger archive from the GitHub **Release** and extract it into `study/`:

```bash
# GitHub Release: https://github.com/open-lakehouse/safe-spark-agents-paper/releases/tag/v1-repro
gh release download v1-repro --repo open-lakehouse/safe-spark-agents-paper --pattern 'ssa-repro-archive.tar.gz'
tar xzf ssa-repro-archive.tar.gz -C study/
```

The archive ships as a release asset (not committed) to keep the repo cloneable; its `MANIFEST.txt` lists
contents, sizes, and the instrument SHA it was produced under. Regenerating from seeds reproduces the same
inputs; the archive additionally pins the exact agent transcripts and outputs from the reported run.
