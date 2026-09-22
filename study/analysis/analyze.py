"""Statistical analysis for the safe-agent study (pre-reg §7).

Runs on `results.jsonl` and emits the headline-number table. Implements the
pre-registered plan exactly:

  * Effect estimate: A-B difference in silent-defect rate, PAIRED BY TASK
    (pre-reg §7). The paired unit is a (task, seed) cell -- because a matched
    seed gives every arm byte-identical input -- so within a cell we can take
    silent(X) - silent(Y) directly.
  * Intervals: bootstrap 95% CI, PERCENTILE method, B = 10,000 resamples,
    RESAMPLING AT THE (task, seed) LEVEL. The CI method, the bootstrap seed, and
    the run count are all reported (pre-reg §7 explicitly closes this rigor gap).
  * Inference: mixed-effects logistic regression silent_defect ~ arm with random
    intercepts for task AND seed (statsmodels BinomialBayesMixedGLM, crossed REs);
    report odds ratios + marginal effects. Primary alpha = 0.05.
  * Multiple comparisons: Holm correction across the 5 arm contrasts
    (A-B, A-B1, A-B2, B-B1, B-B2).
  * H2: paired compute-to-correct (executor-seconds & USD) A (execute-only) vs
    the gated loop; report median, mean, 95% CI, total $ saved, and the % of
    failing iterations intercepted at the dry-run gate.

H2 METRIC SELECTION (pre-reg addendum / threat-model — see DEVIATIONS.md D-7 and
PREREGISTRATION.md §5). The registered H2 primary is the MEASURED
`executor_seconds_to_correct`. That field is the cross-arm-comparable primary
ONLY on the REMOTE/Connect substrate, where every arm is measured by the SAME
stage-diff mechanism (harness/backends/live.py). On the LOCAL substrate it is
NOT comparable and we DELIBERATELY do not pair on it:

  (1) the imperative LocalSparkExecutor snapshots executor-seconds BEFORE its
      SparkSession exists (harness/backends/local.py run_execute), so its
      measured value collapses to None — pairing on it silently drops EVERY
      local pair; and
  (2) even when non-None, imperative measures via the Spark UI /executors
      totalDuration delta while local SDP measures via the Connect stage-diff —
      two DIFFERENT mechanisms that are not comparable across arms.

So on LOCAL backends H2 pairs on `executor_seconds_wallclock_to_correct`, the
UNIFORM `wall_s * instances * busy_fraction` proxy computed by an IDENTICAL
formula for every arm (harness/cost.py), which IS cross-arm comparable. The
choice is made EXPLICITLY by `resolve_h2_metric` (keyed on the run's backend,
threaded from the env sidecar) and recorded in the report under
`meta.h2_metric` and each gated arm's `metric_field` — never a silent default.
An absent/None or unrecognized backend RAISES `H2MetricSelectionError` rather
than guessing the metric from pair availability (which could emit a
non-comparable number); the no-env opt-in is the explicit `--assume-backend`
flag. Fixing the local imperative snapshot-ordering bug is a tracked follow-up;
it feeds only the non-comparable measured secondary and is out of scope here.

statsmodels is only needed for the GLMM. If it is absent, the bootstrap CIs,
the per-contrast paired tests (exact McNemar), the Holm correction, and the H2
analysis all still run, and the GLMM section prints an explicit
`pip install statsmodels` instruction rather than failing. The GLMM is the
declared PRIMARY inference; McNemar is the always-available paired fallback and
is labelled as such.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

BOOTSTRAP_B = 10000
BOOTSTRAP_SEED = 20260623          # reported with results (pre-reg §7 rigor)
ALPHA = 0.05
CONTRASTS = [("A", "B")]   # locked 2-arm design (paper §6.1); A2/B2/B1 retired from the headline
CI_HALF_WIDTH_TARGET = 0.05        # pre-reg §6: 95% CI half-width <= 0.05 on A-B

# H4/H5 (conciseness): declarative (Arm B, SDP) owns a SMALLER decision surface than the
# imperative arm. Under the locked 2-arm design (paper §6.1) the contrast is B-vs-A
# (A2 is withdrawn). The four size metrics are emitted per row by program_metrics.py.
# Reported DIRECTION: imperative - declarative (A - B), so a POSITIVE difference = lines/
# nodes the declarative agent did NOT have to write. Body-only variants exclude the
# mandatory @dp/def/import scaffolding. NOTE: A and B run on different substrates
# (classic Spark vs Connect), but LOC/AST are source-level metrics, substrate-independent.
CONCISENESS_CONTRAST = ("B", "A")
CONCISENESS_METRICS = ("final_program_loc", "final_program_loc_body",
                       "ast_node_count", "ast_node_count_body")

# Harness-fault (instrument failure) exit classes -- these rows are QUARANTINED: an
# instrument failure is never an agent outcome, so it is EXCLUDED from the H1-H4 statistics
# and reported separately (Part B.5). Mirrors harness.schema.HARNESS_FAULT_EXIT_CLASSES;
# imported from there when the harness package is importable (single source of truth),
# with a literal fallback so this analysis script stays runnable standalone.
QUARANTINE_EXIT_CLASS = "HARNESS_ERROR"
# Default TASKS.lock used for the complexity join (item 2); overridable via --tasks.
_DEFAULT_TASKS_LOCK = None
try:  # pragma: no cover - exercised both ways across environments
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
    from harness.schema import HARNESS_FAULT_EXIT_CLASSES as _HF
    from harness.harness_faults import task_complexity_bin as _task_complexity_bin
    HARNESS_FAULT_EXIT_CLASSES = frozenset(_HF)
    _DEFAULT_TASKS_LOCK = _os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "config", "TASKS.lock.json")
except Exception:  # noqa: BLE001
    HARNESS_FAULT_EXIT_CLASSES = frozenset({
        "PROPOSE_TIMEOUT", "PROPOSE_API_ERROR", "PROPOSE_RATE_LIMIT",
        "HARNESS_EXCEPTION", "HARNESS_ERROR"})

    def _task_complexity_bin(task_spec):
        """Standalone fallback (mirrors harness.harness_faults.task_complexity_bin): a
        deterministic complexity bin from the in-scope defect count with named bounds."""
        for key in ("complexity_bin", "complexity", "tier"):
            v = task_spec.get(key)
            if isinstance(v, str) and v.lower() in ("low", "medium", "high"):
                return v.lower()
        n = len(task_spec.get("defects_in_scope") or [])
        return "low" if n <= 3 else ("high" if n >= 5 else "medium")


def is_harness_fault_row(r: Dict[str, Any]) -> bool:
    """True iff this results row is a HARNESS FAULT (instrument failure), so it must be
    excluded from H1-H4 and reported in the quarantine appendix instead."""
    return r.get("exit_class") in HARNESS_FAULT_EXIT_CLASSES


def quarantine_report(quarantined: List[Dict[str, Any]]) -> Dict[str, Any]:
    """The excluded-data (quarantine) appendix: one record per excluded cell with the
    fields the paper needs (task, seed, arm, exit_class, reason), plus counts by arm and
    by underlying reason. `reason` is the specific underlying fault class preserved on the
    row (`harness_fault_reason`), falling back to the row's exit_class for legacy rows."""
    cells = []
    by_arm: Dict[str, int] = defaultdict(int)
    by_reason: Dict[str, int] = defaultdict(int)
    for r in quarantined:
        reason = r.get("harness_fault_reason") or r.get("exit_class")
        cells.append({
            "task": r.get("task"), "seed": r.get("seed"), "arm": r.get("arm"),
            "exit_class": r.get("exit_class"), "reason": reason,
        })
        by_arm[r.get("arm")] += 1
        by_reason[reason] += 1
    return {"n_quarantined": len(cells), "by_arm": dict(by_arm),
            "by_reason": dict(by_reason), "cells": cells}


def _rng(label: str):
    """Deterministic RNG seeded reproducibly from a label (rigor fix).

    Python's builtin hash() is salted per process (PYTHONHASHSEED), so the prior
    `BOOTSTRAP_SEED + hash(key)` made the bootstrap NON-reproducible across runs.
    zlib.crc32 is a fixed, process-independent hash, so the same (results, seed)
    always yields the same intervals.
    """
    import zlib
    return np.random.default_rng(BOOTSTRAP_SEED + (zlib.crc32(label.encode()) % 1_000_000))


def required_n_for_halfwidth(idx, x: str = "A", y: str = "B",
                             half_width: float = CI_HALF_WIDTH_TARGET, z: float = 1.96):
    """Pre-reg §6 power rule: N (paired (task,seed) cells) needed for a 95% CI
    half-width <= `half_width` on the x-y silent-defect-rate difference.

    Half-width ~= z * sd(paired diff) / sqrt(N)  =>  N >= (z*sd/half_width)^2.
    Returns (required_n, observed_sd, current_n). current_n is the paired-cell
    count available now; the caller refuses to claim the headline N unless
    current_n >= required_n.
    """
    pu = paired_units(idx, x, y)
    if len(pu) < 2:
        return None, None, len(pu)
    d = np.array([a - b for a, b in pu], dtype=float)
    sd = float(d.std(ddof=1))
    req = int(math.ceil((z * sd / half_width) ** 2)) if sd > 0 else 1
    return req, sd, len(pu)


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------
def load_all_rows(path: str) -> List[Dict[str, Any]]:
    """Every row in results.jsonl, INCLUDING quarantined harness-fault rows. Used to
    build the quarantine report; the analysis path uses `load_rows` (filtered) instead."""
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def load_rows(path: str) -> List[Dict[str, Any]]:
    """The ANALYSIS row set: every results.jsonl row EXCEPT harness-fault (instrument)
    rows, which are SKIPPED AT LOAD TIME. Enforcing the quarantine filter here -- not
    only at the H1-H4 call sites -- guarantees a quarantined row can NEVER reach an agent
    statistic (it cannot depress silent_defect, inflate a denominator, or enter the GLMM
    frame) no matter which loader a future analysis path calls. The excluded rows are read
    separately via `load_all_rows` for the quarantine appendix."""
    return [r for r in load_all_rows(path) if not is_harness_fault_row(r)]


# ---------------------------------------------------------------------------
# complexity join (item 2): make a per-task complexity column available for the
# later H4 (paradigm x complexity) model. We do NOT build H4 here -- we only join
# the column so it is present in the analysis frame.
# ---------------------------------------------------------------------------
def task_complexity_map(tasks_path: str) -> Dict[str, Dict[str, Any]]:
    """`{task_id: {"complexity_bin": str, "complexity_score": int}}` parsed from
    TASKS.lock.json. `complexity_score` is the in-scope defect count (a numeric complexity
    proxy); `complexity_bin` is the SAME named bin the harness-fault circuit breaker uses
    (`harness.harness_faults.task_complexity_bin`), so analysis and runtime agree."""
    with open(tasks_path) as f:
        lock = json.load(f)
    out: Dict[str, Dict[str, Any]] = {}
    for t in lock.get("tasks", []):
        out[t["id"]] = {"complexity_bin": _task_complexity_bin(t),
                        "complexity_score": len(t.get("defects_in_scope") or [])}
    return out


def join_complexity(rows: List[Dict[str, Any]], tasks_path: Optional[str]) -> List[Dict[str, Any]]:
    """Attach `complexity_bin` + `complexity_score` onto each row (in place + returned),
    joined from TASKS.lock by task id, so the analysis dataframe carries the complexity
    columns H4 will need. Rows whose task is absent get None. Never raises (a missing/
    unreadable lock just leaves the columns None)."""
    cmap: Dict[str, Dict[str, Any]] = {}
    if tasks_path and os.path.exists(tasks_path):
        try:
            cmap = task_complexity_map(tasks_path)
        except Exception:  # noqa: BLE001
            cmap = {}
    for r in rows:
        meta = cmap.get(r.get("task"), {})
        r["complexity_bin"] = meta.get("complexity_bin")
        r["complexity_score"] = meta.get("complexity_score")
    return rows


def analysis_frame(rows: List[Dict[str, Any]]):
    """A pandas DataFrame of the ANALYSIS rows with the columns later models need, INCLUDING
    the joined `complexity_bin` / `complexity_score` for the future H4 (paradigm x
    complexity) model. pandas-guarded: returns None if pandas is unavailable. Call
    `join_complexity` on `rows` first so the complexity columns are populated."""
    try:
        import pandas as pd
    except Exception:  # noqa: BLE001
        return None
    return pd.DataFrame({
        "silent": [1 if r.get("silent_defect") else 0 for r in rows],
        "arm": [r.get("arm") for r in rows],
        "task": [r.get("task") for r in rows],
        "seed": [str(r.get("seed")) for r in rows],
        "complexity_bin": [r.get("complexity_bin") for r in rows],
        "complexity_score": [r.get("complexity_score") for r in rows],
    })


def _count_by(rows: List[Dict[str, Any]], key: str) -> Dict[Any, int]:
    c: Dict[Any, int] = defaultdict(int)
    for r in rows:
        c[r.get(key)] += 1
    return c


def cell_index(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, int], Dict[str, Dict[str, Any]]]:
    """(task, seed) -> {arm: row}. The paired-unit index."""
    idx: Dict[Tuple[str, int], Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for r in rows:
        idx[(r["task"], int(r["seed"]))][r["arm"]] = r
    return idx


# ---------------------------------------------------------------------------
# per-arm silent-defect rate + bootstrap CI
# ---------------------------------------------------------------------------
def arm_rate(rows: List[Dict[str, Any]], arm: str) -> Tuple[float, int, int]:
    vals = [1 if r["silent_defect"] else 0 for r in rows if r["arm"] == arm]
    n = len(vals)
    k = sum(vals)
    return (k / n if n else float("nan"), k, n)


def bootstrap_arm_rate_ci(idx, arm: str, rng) -> Optional[Tuple[float, float]]:
    units = [(c, cell[arm]) for c, cell in idx.items() if arm in cell]
    if not units:
        return None
    vals = np.array([1 if r["silent_defect"] else 0 for _, r in units], dtype=float)
    n = len(vals)
    boot = np.empty(BOOTSTRAP_B)
    for b in range(BOOTSTRAP_B):
        samp = rng.integers(0, n, n)
        boot[b] = vals[samp].mean()
    return (float(np.percentile(boot, 2.5)), float(np.percentile(boot, 97.5)))


# ---------------------------------------------------------------------------
# paired contrast: A-B difference in silent-defect rate + bootstrap CI
# ---------------------------------------------------------------------------
def paired_units(idx, x: str, y: str):
    """Cells where BOTH arms ran -> list of (silent_x, silent_y)."""
    out = []
    for c, cell in idx.items():
        if x in cell and y in cell:
            out.append((1 if cell[x]["silent_defect"] else 0,
                        1 if cell[y]["silent_defect"] else 0))
    return out


def contrast_diff(idx, x: str, y: str) -> Optional[float]:
    pu = paired_units(idx, x, y)
    if not pu:
        return None
    return float(np.mean([a - b for a, b in pu]))


def bootstrap_contrast_ci(idx, x: str, y: str, rng) -> Optional[Tuple[float, float, float]]:
    pu = paired_units(idx, x, y)
    if not pu:
        return None
    d = np.array([a - b for a, b in pu], dtype=float)
    n = len(d)
    boot = np.empty(BOOTSTRAP_B)
    for b in range(BOOTSTRAP_B):
        samp = rng.integers(0, n, n)
        boot[b] = d[samp].mean()
    return (float(d.mean()), float(np.percentile(boot, 2.5)), float(np.percentile(boot, 97.5)))


# ---------------------------------------------------------------------------
# H5: B-vs-A2 conciseness contrast (paired over (task, seed); same bootstrap machinery)
# ---------------------------------------------------------------------------
def paired_metric_units(idx, x: str, y: str, metric: str):
    """Cells where BOTH arms completed AND both carry a non-null `metric` ->
    list of (value_x, value_y). A run that never completed has a null metric and is
    skipped (there is no final accepted program to measure), exactly like the
    silent-defect pairing skips arms that did not run."""
    out = []
    for _, cell in idx.items():
        if x not in cell or y not in cell:
            continue
        vx = cell[x].get(metric)
        vy = cell[y].get(metric)
        if vx is None or vy is None:
            continue
        out.append((float(vx), float(vy)))
    return out


def conciseness_metric_contrast(idx, x: str, y: str, metric: str, rng) -> Dict[str, Any]:
    """Paired y-x conciseness contrast for ONE metric, reusing the percentile
    bootstrap. Reports the difference as `y - x` (imperative A2 minus declarative B),
    so a POSITIVE value = decision surface the declarative agent did NOT write."""
    pu = paired_metric_units(idx, x, y, metric)
    n = len(pu)
    if n == 0:
        return {"metric": metric, "n_pairs": 0, "note": "no paired completed cells"}
    diffs = np.array([vy - vx for vx, vy in pu], dtype=float)   # imperative - declarative
    mean_x = float(np.mean([vx for vx, _ in pu]))               # declarative (B)
    mean_y = float(np.mean([vy for _, vy in pu]))               # imperative (A)
    boot = np.empty(BOOTSTRAP_B)
    for b in range(BOOTSTRAP_B):
        samp = rng.integers(0, n, n)
        boot[b] = diffs[samp].mean()
    pct = (float(diffs.mean()) / mean_y) if mean_y else None    # fraction smaller than imperative
    return {
        "metric": metric,
        "n_pairs": n,
        "mean_declarative": mean_x,
        "mean_imperative": mean_y,
        "mean_diff_imp_minus_decl": float(diffs.mean()),
        "median_diff_imp_minus_decl": float(np.median(diffs)),
        "bootstrap_ci95_diff": [float(np.percentile(boot, 2.5)),
                                float(np.percentile(boot, 97.5))],
        "pct_smaller_than_imperative": pct,
    }


def conciseness_analysis(idx) -> Dict[str, Any]:
    """H5 headline: the B-vs-A2 conciseness contrast across all four size metrics,
    paired over (task, seed). Empty `per_metric` (and n_pairs 0) when A2 or B is
    absent / never completed -- never raises, so the analysis runs on any results set."""
    x, y = CONCISENESS_CONTRAST
    per_metric = {
        m: conciseness_metric_contrast(idx, x, y, m, _rng(f"conciseness:{x}-{y}:{m}"))
        for m in CONCISENESS_METRICS
    }
    n_pairs = max((d.get("n_pairs", 0) for d in per_metric.values()), default=0)
    return {
        "contrast": f"{x}-vs-{y}",
        "declarative_arm": x,
        "imperative_arm": y,
        "direction": "difference reported as imperative(A) - declarative(B); positive = declarative is smaller",
        "n_pairs": n_pairs,
        "per_metric": per_metric,
    }


# ---------------------------------------------------------------------------
# exact McNemar paired test (always available; Holm input fallback / cross-check)
# ---------------------------------------------------------------------------
def mcnemar_p(idx, x: str, y: str) -> Optional[float]:
    pu = paired_units(idx, x, y)
    if not pu:
        return None
    b = sum(1 for a, c in pu if a == 1 and c == 0)   # x silent, y not
    c = sum(1 for a, c in pu if a == 0 and c == 1)   # y silent, x not
    nd = b + c
    if nd == 0:
        return 1.0
    from scipy import stats
    # exact two-sided McNemar via binomial
    p = 2.0 * stats.binom.cdf(min(b, c), nd, 0.5)
    return float(min(1.0, p))


def holm(pvals: Dict[str, float]) -> Dict[str, float]:
    """Holm-Bonferroni step-down adjusted p-values."""
    items = [(k, v) for k, v in pvals.items() if v is not None]
    items.sort(key=lambda kv: kv[1])
    m = len(items)
    adj: Dict[str, float] = {}
    prev = 0.0
    for i, (k, p) in enumerate(items):
        a = (m - i) * p
        a = max(prev, min(1.0, a))   # enforce monotonicity + cap at 1
        adj[k] = a
        prev = a
    for k, v in pvals.items():
        if v is None:
            adj[k] = float("nan")
    return adj


# ---------------------------------------------------------------------------
# GLMM (primary inference) -- statsmodels, guarded
# ---------------------------------------------------------------------------
def fit_glmm(rows: List[Dict[str, Any]], ref: str):
    """BinomialBayesMixedGLM: silent_defect ~ C(arm, ref) + (1|task) + (1|seed).

    Returns (summary_dict | None, error_message | None).
    """
    try:
        import pandas as pd
    except Exception as e:  # noqa: BLE001
        return None, (f"statsmodels/pandas not installed ({e}); GLMM skipped. "
                      "Install with: pip install -r analysis/requirements.txt")
    arms = sorted({r["arm"] for r in rows})
    if ref not in arms or len(arms) < 2:
        return None, f"cannot fit GLMM: need >=2 arms incl. reference {ref!r}; have {arms}"
    df = pd.DataFrame({
        "silent": [1 if r["silent_defect"] else 0 for r in rows],
        "arm": [r["arm"] for r in rows],
        "task": [r["task"] for r in rows],
        "seed": [str(r["seed"]) for r in rows],
    })
    # degenerate-outcome guard: VB GLMM needs variation in the response
    if df["silent"].nunique() < 2:
        return None, ("GLMM not identifiable: silent_defect has no variation "
                      f"(all {int(df['silent'].iloc[0])}); report rates/CIs only.")
    try:
        from scipy import stats
        from statsmodels.genmod.bayes_mixed_glm import BinomialBayesMixedGLM
        vc = {"task": "0 + C(task)", "seed": "0 + C(seed)"}
        model = BinomialBayesMixedGLM.from_formula(
            f"silent ~ C(arm, Treatment(reference={ref!r}))", vc, df)
        res = model.fit_vb()
        names = list(res.model.exog_names)
        b0 = float(res.fe_mean[names.index("Intercept")]) if "Intercept" in names else 0.0

        def sigmoid(z):
            return 1.0 / (1.0 + math.exp(-z))

        out = {}
        for i, nm in enumerate(names):
            if nm.startswith("C(arm"):
                coef = float(res.fe_mean[i])
                sd = float(res.fe_sd[i])
                z = coef / sd if sd > 0 else float("nan")
                p = float(2 * (1 - stats.norm.cdf(abs(z)))) if sd > 0 else float("nan")
                arm = nm.split("T.")[-1].rstrip("]")
                # B6: model-based AVERAGE MARGINAL EFFECT on the probability scale,
                # with the crossed random intercepts held at their posterior mean
                # (0): AME = P(silent | arm) - P(silent | ref) at the FE means.
                ame = sigmoid(b0 + coef) - sigmoid(b0)
                out[arm] = {"odds_ratio": math.exp(coef), "coef": coef, "sd": sd,
                            "z": z, "p": p, "ame": ame}
        return out, None
    except Exception as e:  # noqa: BLE001
        return None, f"GLMM fit failed: {e}"


def glmm_contrasts(rows: List[Dict[str, Any]]):
    """Extract the 5 pre-registered arm-contrast p-values FROM THE GLMM (B5).

    A-vs-{B,B1,B2} come from the reference=A fit; B-vs-{B1,B2} from reference=B.
    Returns (contrasts_dict | None, error). These p-values -- not the McNemar
    fallback -- are what build_report Holm-corrects (pre-reg §7 declares the GLMM
    as the inference; McNemar is only used if the GLMM cannot be fitted).
    """
    gA, errA = fit_glmm(rows, ref="A")
    if gA is None:
        return None, errA
    gB, _ = fit_glmm(rows, ref="B")
    out: Dict[str, Dict[str, Any]] = {}
    for arm in ("B", "B1", "B2"):
        if arm in gA:
            out[f"A-{arm}"] = {"p": gA[arm]["p"], "odds_ratio": gA[arm]["odds_ratio"],
                               "coef": gA[arm]["coef"], "ame": gA[arm]["ame"]}
    if gB:
        for arm in ("B1", "B2"):
            if arm in gB:
                out[f"B-{arm}"] = {"p": gB[arm]["p"], "odds_ratio": gB[arm]["odds_ratio"],
                                   "coef": gB[arm]["coef"], "ame": gB[arm]["ame"]}
    return out, None


def observed_rates(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """Observed per-arm silent-defect rate -- DESCRIPTIVE only (not the AME)."""
    return {arm: arm_rate(rows, arm)[0] for arm in sorted({r["arm"] for r in rows})}


# ---------------------------------------------------------------------------
# H2: compute-to-correct, A (execute-only) vs gated loop
# ---------------------------------------------------------------------------
def gated_arms(rows: List[Dict[str, Any]], arms_meta: Optional[Dict[str, Any]]) -> List[str]:
    if arms_meta:
        return [a for a, sig in arms_meta.items() if sig.get("dry_run_gate")]
    # fallback: B and B2 are the gated arms per pre-reg §3
    present = {r["arm"] for r in rows}
    return [a for a in ("B", "B2") if a in present]


# --- H2 compute-to-correct metric selection (cross-arm comparability) --------
# The two compute-to-correct fields a result row can carry, and which backends
# each one is the COMPARABLE H2 primary for. See the module docstring + D-7.
H2_METRIC_MEASURED = "executor_seconds_to_correct"            # measured stage-diff (remote/Connect)
H2_METRIC_WALLCLOCK = "executor_seconds_wallclock_to_correct"  # uniform wall_s*slots proxy (local)
LOCAL_BACKENDS = frozenset({"local"})   # imperative=classic local[*]; SDP=local Connect
REMOTE_BACKENDS = frozenset({"live"})   # cluster Spark Connect; measured field is comparable
KNOWN_H2_BACKENDS = LOCAL_BACKENDS | REMOTE_BACKENDS


class H2MetricSelectionError(ValueError):
    """Raised when the H2 compute-to-correct metric cannot be selected from a KNOWN
    backend -- we refuse to emit a possibly-wrong (non-comparable) H2 number."""


def resolve_h2_metric(backend: Optional[str], idx, gated: List[str]) -> Tuple[str, Dict[str, Any]]:
    """Choose the H2 compute-to-correct field EXPLICITLY from a KNOWN backend.

    Returns (field_name, metadata) where metadata records the field, a human
    label, the backend, and the selection rationale -- surfaced in the report so
    a reader can see WHY a given metric was used.

      * LOCAL backends  -> the uniform wall-clock executor-seconds proxy
        (H2_METRIC_WALLCLOCK), because the measured field is non-comparable
        across the classic-vs-Connect local engines and collapses to None for
        the imperative arm (see module docstring (1)/(2)).
      * REMOTE backends -> the measured stage-diff field (H2_METRIC_MEASURED),
        which IS measured identically across arms on the cluster.

    There is DELIBERATELY no data-driven fallback: inferring the metric from
    "which field happens to have pairs" can pick measured for an unknown-backend
    LOCAL run (a non-comparable number) or the proxy for an unknown-backend REMOTE
    run (the wrong primary) -- both emit a plausible-but-wrong H2. So an
    absent/None or unrecognized backend RAISES `H2MetricSelectionError`; the
    caller must supply a known backend (the runner writes it into the env sidecar;
    `--assume-backend {local,live}` is the explicit no-env opt-in). `idx`/`gated`
    are unused now but kept in the signature for callers/tests.
    """
    del idx, gated  # no data-driven inference: selection is backend-driven only
    if backend in LOCAL_BACKENDS:
        return H2_METRIC_WALLCLOCK, {
            "field": H2_METRIC_WALLCLOCK,
            "label": "uniform wall-clock executor-seconds proxy (cross-arm comparable)",
            "backend": backend,
            "selection": f"backend={backend!r}: local substrate measured field is not cross-arm comparable",
        }
    if backend in REMOTE_BACKENDS:
        return H2_METRIC_MEASURED, {
            "field": H2_METRIC_MEASURED,
            "label": "measured cluster executor-seconds (stage-diff)",
            "backend": backend,
            "selection": f"backend={backend!r}: remote/Connect measures executor-seconds identically across arms",
        }
    raise H2MetricSelectionError(
        f"cannot select an H2 compute-to-correct metric: backend={backend!r} is not one of "
        f"{sorted(KNOWN_H2_BACKENDS)}. H2 metric selection requires a KNOWN backend so the "
        "number is cross-arm comparable; re-run analysis with --env <sidecar> whose top-level "
        "'backend' is one of {local, live}, or pass --assume-backend {local,live} explicitly. "
        "Refusing to guess the metric from pair availability (it can emit a non-comparable H2)."
    )


def _h2_pairs(idx, g: str, complete_case: bool, metric_field: str):
    """Paired (A, g) compute-to-correct over (task,seed) cells, on `metric_field`.

    `metric_field` is the compute-to-correct field to pair on -- REQUIRED, and
    must come from `resolve_h2_metric` (measured stage-diff for remote, the
    uniform wall-clock proxy for local). There is DELIBERATELY no default: a
    defaulted metric would silently recreate the original bug (the old code
    hard-coded the measured field, which is None for every local imperative row
    -> all local pairs lost). Passing None/empty raises `H2MetricSelectionError`.

    B9: by default (complete_case=False) this is INTENTION-TO-TREAT -- it includes
    every cell where both A and g ran, using the to-correct field (which the
    runner imputes to total compute spent for a run that never reached correct, a
    conservative lower bound). complete_case=True restricts to cells where BOTH
    arms actually reached a correct output (reached_correct), the sensitivity
    analysis. Conditioning H2 only on success (the old behaviour) is the bias B9
    flags; we report BOTH and never silently drop failures.
    """
    if not metric_field:
        raise H2MetricSelectionError(
            "_h2_pairs requires an explicit metric_field from resolve_h2_metric "
            "(one of H2_METRIC_MEASURED / H2_METRIC_WALLCLOCK); refusing to default "
            "to the measured field (it silently distorts/drops local H2 pairs).")
    pairs = []
    for _, cell in idx.items():
        if "A" not in cell or g not in cell:
            continue
        if complete_case and not (cell["A"].get("reached_correct") and cell[g].get("reached_correct")):
            continue
        a_cost = cell["A"].get(metric_field)
        g_cost = cell[g].get(metric_field)
        if a_cost is None or g_cost is None:
            continue
        pairs.append((a_cost, g_cost, cell["A"].get("usd", 0.0), cell[g].get("usd", 0.0)))
    return pairs


def _h2_summary(pairs, label: str, g: str):
    if not pairs:
        return {"mode": label, "n_pairs": 0, "note": "no matched cells"}
    diffs = np.array([a - b for a, b, _, _ in pairs], dtype=float)
    usd_saved = float(sum(ua - ug for _, _, ua, ug in pairs))
    n = len(diffs)
    rng = _rng(f"h2:{g}:{label}")
    boot = np.array([diffs[rng.integers(0, n, n)].mean() for _ in range(BOOTSTRAP_B)])
    return {
        "mode": label, "n_pairs": n,
        "median_exec_s_saved": float(np.median(diffs)),
        "mean_exec_s_saved": float(diffs.mean()),
        "ci95_exec_s_saved": [float(np.percentile(boot, 2.5)), float(np.percentile(boot, 97.5))],
        "total_usd_saved_at_scale": usd_saved,
    }


def h2_analysis(idx, rows, arms_meta, metric_field: str) -> Dict[str, Any]:
    """Per-gated-arm H2 compute-to-correct on `metric_field` (REQUIRED).

    `metric_field` must be a backend-resolved choice from `resolve_h2_metric`;
    there is no default so no caller can compute H2 on an unstated metric (which
    would distort/drop local rows). Passing None/empty raises
    `H2MetricSelectionError`.
    """
    if not metric_field:
        raise H2MetricSelectionError(
            "h2_analysis requires an explicit metric_field from resolve_h2_metric "
            "(backend-resolved); refusing to default to the measured field, which "
            "silently distorts/drops local H2 pairs.")
    out: Dict[str, Any] = {}
    for g in gated_arms(rows, arms_meta):
        gi = [r for r in rows if r["arm"] == g]
        fi = sum(r.get("failing_iterations", 0) for r in gi)
        di = sum(r.get("dry_run_intercepts", 0) for r in gi)
        out[g] = {
            "metric_field": metric_field,
            "itt": _h2_summary(_h2_pairs(idx, g, False, metric_field), "intention_to_treat", g),
            "complete_case": _h2_summary(_h2_pairs(idx, g, True, metric_field), "complete_case", g),
            "failing_iterations": fi,
            "dry_run_intercepts": di,
            "intercept_fraction": (di / fi) if fi else None,
        }
    return out


# ---------------------------------------------------------------------------
# headline table
# ---------------------------------------------------------------------------
# --- §9 registered error taxonomy: catch-stage by defect-class group --------------
# Pre-registered in SECTION1_DATA_AND_METHODOLOGY.md §9 (2026-06-29). The HEADLINE is
# WHERE structural & runtime errors are caught -- `dry_run` (gate, pre-execution),
# `runtime` (during execute), or `never` (shipped) -- NOT the silent-defect rate (that
# is the predicted-null CONTROL). Counted at the defect/iteration level so a structural
# error the gate catches and the agent then fixes STILL counts (anti-bypass rule §9.2);
# nothing a "safety" mechanism intercepts can vanish from the measurement.
D_GROUPS = {"structural": ("D1", "D4", "D5"),        # gate-catchable (loud, pre-exec)
            "semantic":   ("D2", "D6", "D7", "D8"),  # un-gateable silent residue (CONTROL)
            "state":      ("D3", "D9")}
DETECT_STAGES = ("dry_run", "runtime", "never")
_D_TO_GROUP = {d: g for g, ds in D_GROUPS.items() for d in ds}


def error_taxonomy(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Per-arm catch-stage distribution (P1/P3) from `per_defect_detection`
    (values dry_run/runtime/never/n_a) grouped by defect-class group, plus structural
    dry-run intercepts and per-iteration runtime/gate error events (P2). All values come
    from THIS run's rows -- never the pre-fairness pilot data (anti-drift §9.2)."""
    arms = sorted({r["arm"] for r in rows})
    out: Dict[str, Any] = {}
    for a in arms:
        ar = [r for r in rows if r.get("arm") == a]
        groups = {g: {s: 0 for s in DETECT_STAGES} for g in D_GROUPS}
        for r in ar:
            for d, stage in (r.get("per_defect_detection") or {}).items():
                g = _D_TO_GROUP.get(d)
                if g is not None and stage in DETECT_STAGES:   # skip 'n/a' (defect not in scope)
                    groups[g][stage] += 1
        runtime_errs = gate_errs = 0
        for r in ar:
            for it in (r.get("per_iteration") or []):
                if (it.get("execute") or {}).get("error_class"):
                    runtime_errs += 1
                if (it.get("gate") or {}).get("error_class"):
                    gate_errs += 1
        out[a] = {
            "by_group": groups,
            "dry_run_intercepts": sum((r.get("dry_run_intercepts") or 0) for r in ar),
            "runtime_error_events": runtime_errs,
            "gate_error_events": gate_errs,
            "n_cells": len(ar),
        }
    return out


def build_report(path: str, arms_meta: Optional[Dict[str, Any]],
                 backend: Optional[str] = None,
                 tasks_path: Optional[str] = None) -> Dict[str, Any]:
    all_rows = load_all_rows(path)
    # QUARANTINE (Part B.5): a HARNESS-FAULT row is an INSTRUMENT failure, never an agent
    # outcome -- EXCLUDE it from every H1-H4 computation below and report it separately.
    # (load_rows() ALSO enforces this filter at load time; we read all rows here only to
    # build the quarantine appendix.)
    quarantined = [r for r in all_rows if is_harness_fault_row(r)]
    rows = [r for r in all_rows if not is_harness_fault_row(r)]
    quarantine = quarantine_report(quarantined)
    # item 2: join the per-task complexity column onto the analyzed rows so the analysis
    # frame carries complexity_bin/score for the later H4 (paradigm x complexity) model.
    if tasks_path is None:
        tasks_path = _DEFAULT_TASKS_LOCK
    join_complexity(rows, tasks_path)
    idx = cell_index(rows)

    arms = sorted({r["arm"] for r in rows})
    per_arm = {}
    for a in arms:
        rate, k, n = arm_rate(rows, a)
        ci = bootstrap_arm_rate_ci(idx, a, _rng(f"arm:{a}"))
        per_arm[a] = {"silent_defect_rate": rate, "k": k, "n": n, "ci95": ci}

    # --- per-contrast bootstrap CIs + McNemar (descriptive / fallback) -----
    contrasts = {}
    for x, y in CONTRASTS:
        key = f"{x}-{y}"
        cc = bootstrap_contrast_ci(idx, x, y, _rng(f"contrast:{key}"))
        contrasts[key] = {
            "diff_silent_rate": (None if cc is None else cc[0]),
            "bootstrap_ci95": (None if cc is None else [cc[1], cc[2]]),
            "mcnemar_p": mcnemar_p(idx, x, y),
        }

    # --- B5: Holm correction over the PRE-REGISTERED GLMM contrast p-values --
    gc, gc_err = glmm_contrasts(rows)
    if gc is not None:
        p_source = "glmm"
        raw_p = {k: gc[k]["p"] for k in gc}
        for key in contrasts:
            if key in gc:
                contrasts[key]["glmm_p"] = gc[key]["p"]
                contrasts[key]["odds_ratio"] = gc[key]["odds_ratio"]
                contrasts[key]["ame"] = gc[key]["ame"]
    else:
        p_source = "mcnemar_fallback"     # GLMM unavailable -> labelled fallback
        raw_p = {k: contrasts[k]["mcnemar_p"] for k in contrasts}
    holm_adj = holm(raw_p)
    for key in contrasts:
        contrasts[key]["holm_adjusted_p"] = holm_adj.get(key)
        contrasts[key]["holm_p_source"] = p_source
        p = raw_p.get(key)
        contrasts[key]["significant_holm"] = (
            None if p is None else bool(holm_adj.get(key, 1.0) < ALPHA))

    glmm_A, errA = fit_glmm(rows, ref="A")
    glmm_B, errB = fit_glmm(rows, ref="B")
    glmm = {"ref_A": glmm_A, "ref_A_error": errA,
            "ref_B": glmm_B, "ref_B_error": errB,
            "ame_source": ("glmm" if glmm_A else "unavailable"),
            "observed_rates_descriptive": observed_rates(rows),
            "p_value_source_for_holm": p_source,
            "glmm_contrast_error": gc_err}

    # --- B7: power rule (pre-reg §6) ---------------------------------------
    req_n, sd, cur_n = required_n_for_halfwidth(idx, "A", "B")
    power = {
        "ci_half_width_target": CI_HALF_WIDTH_TARGET,
        "observed_sd_paired_AminusB": sd,
        "required_n_cells": req_n,
        "current_n_cells": cur_n,
        "meets_power": (req_n is not None and cur_n >= req_n),
        "rule": "N >= (1.96*sd/0.05)^2 on the A-B paired silent-defect-rate diff",
    }
    headline_n_valid = power["meets_power"]

    # --- H2: pick the cross-arm-comparable compute-to-correct metric EXPLICITLY -
    # Only when there ARE gated arms to compute H2 for: with no gated arm there is no H2
    # contrast, so we must NOT demand a resolvable backend (a backend-less quarantine /
    # complexity / A2-only report is still valid). When gated arms exist, keep the STRICT
    # backend-keyed selection (it RAISES on an unresolvable backend rather than emit a
    # non-comparable H2 -- the runner writes the backend into the env sidecar, or pass
    # --assume-backend).
    _gated = gated_arms(rows, arms_meta)
    if _gated:
        h2_field, h2_metric_meta = resolve_h2_metric(backend, idx, _gated)
        h2 = h2_analysis(idx, rows, arms_meta, metric_field=h2_field)
    else:
        h2_field, h2 = None, {}
        h2_metric_meta = {"field": None,
                          "label": "n/a (no gated arms present)",
                          "backend": backend,
                          "selection": "no gated arms present: H2 not applicable"}
    # --- H5 (from #35): conciseness B-vs-A2 ---------------------------------
    h5 = conciseness_analysis(idx)

    return {
        "meta": {
            "results_path": path,
            "n_rows": len(rows),
            "n_rows_total": len(all_rows),
            "n_quarantined": quarantine["n_quarantined"],
            "n_cells": len(idx),
            "arms": arms,
            "backend": backend,
            "bootstrap_B": BOOTSTRAP_B,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "ci_method": "percentile",
            "resample_unit": "(task, seed)",
            "alpha": ALPHA,
            "multiple_comparison": "Holm across 5 arm contrasts",
            "holm_p_value_source": p_source,
            "headline_n_valid": headline_n_valid,
            "h2_metric": h2_metric_meta,
            "quarantine_excluded_from_H1_H4": True,
            "complexity_join": {
                "tasks_path": tasks_path,
                "n_rows_with_complexity": sum(1 for r in rows if r.get("complexity_bin")),
                "by_bin": dict(_count_by(rows, "complexity_bin")),
            },
        },
        "H1_per_arm": per_arm,
        "H1_contrasts": contrasts,
        "H1_glmm": glmm,
        "H1_power": power,
        "H2_compute_to_correct": h2,
        "H5_conciseness": h5,
        "error_taxonomy": error_taxonomy(rows),   # §9 pre-registered headline (P1/P2/P3)
        "quarantine": quarantine,
    }


def render_markdown(rep: Dict[str, Any]) -> str:
    m = rep["meta"]
    L = []
    L.append("# Safe-agent study — headline numbers\n")
    L.append(f"- rows: **{m['n_rows']}**  |  (task,seed) cells: **{m['n_cells']}**  |  arms: {m['arms']}")
    L.append(f"- CI: bootstrap **{m['ci_method']}**, B=**{m['bootstrap_B']}**, "
             f"seed=**{m['bootstrap_seed']}**, resample unit=**{m['resample_unit']}**")
    L.append(f"- inference: mixed-effects logistic (random intercepts task+seed); "
             f"Holm across 5 contrasts; α={m['alpha']}\n")

    L.append("## H1 — silent-defect rate by arm\n")
    L.append("| arm | silent-defect rate | k/n | 95% CI (bootstrap) |")
    L.append("|---|---|---|---|")
    for a, d in rep["H1_per_arm"].items():
        ci = d["ci95"]
        cis = "—" if ci is None else f"[{ci[0]:.3f}, {ci[1]:.3f}]"
        L.append(f"| {a} | {d['silent_defect_rate']:.3f} | {d['k']}/{d['n']} | {cis} |")

    psrc = m.get("holm_p_value_source", "?")
    L.append(f"\n## H1 — paired contrasts (Holm over the **{psrc}** contrast p-values)\n")
    if psrc != "glmm":
        L.append("_GLMM unavailable; Holm applied to the **McNemar fallback** "
                 "p-values (labelled). Install statsmodels for the pre-registered "
                 "GLMM inference._\n")
    L.append("| contrast | Δ rate | 95% CI (bootstrap) | OR | p (source) | Holm p | sig (α=.05) |")
    L.append("|---|---|---|---|---|---|---|")
    for key, d in rep["H1_contrasts"].items():
        diff = "—" if d["diff_silent_rate"] is None else f"{d['diff_silent_rate']:+.3f}"
        ci = d["bootstrap_ci95"]
        cis = "—" if ci is None else f"[{ci[0]:+.3f}, {ci[1]:+.3f}]"
        orr = "—" if "odds_ratio" not in d else f"{d['odds_ratio']:.3f}"
        praw = d.get("glmm_p", d.get("mcnemar_p"))
        ps = "—" if praw is None else f"{praw:.4f}"
        hp = d["holm_adjusted_p"]
        hps = "—" if hp is None or (isinstance(hp, float) and math.isnan(hp)) else f"{hp:.4f}"
        sig = {True: "YES", False: "no", None: "—"}[d["significant_holm"]]
        L.append(f"| {key} | {diff} | {cis} | {orr} | {ps} | {hps} | {sig} |")

    L.append("\n## H1 — GLMM odds ratios + average marginal effects (primary inference)\n")
    g = rep["H1_glmm"]
    if g["ref_A"]:
        L.append("Reference = Arm A. OR<1 ⇒ FEWER silent defects than A. AME is the "
                 "model-based average marginal effect on P(silent_defect) (RE at 0).\n")
        L.append("| arm vs A | odds ratio | coef | AME (Δ prob) | posterior p |")
        L.append("|---|---|---|---|---|")
        for arm, d in g["ref_A"].items():
            L.append(f"| {arm} | {d['odds_ratio']:.3f} | {d['coef']:+.3f} | {d['ame']:+.3f} | {d['p']:.4f} |")
        if g["ref_B"]:
            L.append("\nReference = Arm B (B-vs-B1 / B-vs-B2 ablation contrasts):\n")
            L.append("| arm vs B | odds ratio | coef | AME (Δ prob) | posterior p |")
            L.append("|---|---|---|---|---|")
            for arm, d in g["ref_B"].items():
                if arm in ("B1", "B2"):
                    L.append(f"| {arm} | {d['odds_ratio']:.3f} | {d['coef']:+.3f} | {d['ame']:+.3f} | {d['p']:.4f} |")
        L.append("\nObserved per-arm silent-defect rates (DESCRIPTIVE, not the AME):")
        L.append("  " + ", ".join(f"{a}={r:.3f}" for a, r in g["observed_rates_descriptive"].items()))
    else:
        L.append(f"_GLMM not fitted: {g['ref_A_error']}_")
        L.append("\nObserved silent-defect rate per arm (descriptive):\n")
        L.append("| arm | rate |")
        L.append("|---|---|")
        for a, p in g["observed_rates_descriptive"].items():
            L.append(f"| {a} | {p:.3f} |")

    pw = rep["H1_power"]
    L.append("\n## H1 — power / sample-size rule (pre-reg §6)\n")
    L.append(f"- target: 95% CI half-width ≤ **{pw['ci_half_width_target']}** on A−B")
    sds = "—" if pw["observed_sd_paired_AminusB"] is None else f"{pw['observed_sd_paired_AminusB']:.3f}"
    L.append(f"- observed sd(A−B paired) = **{sds}**  →  required N = "
             f"**{pw['required_n_cells']}** (task,seed) cells; have **{pw['current_n_cells']}**")
    L.append(f"- **meets power: {pw['meets_power']}** — `headline_n_valid={m.get('headline_n_valid')}` "
             f"(a headline N below required must NOT be claimed)\n")

    # --- §9 registered error taxonomy (PRE-REGISTERED headline) -------------
    et = rep.get("error_taxonomy", {})
    L.append("\n## §9 — Error taxonomy: catch-stage by class group (PRE-REGISTERED headline)\n")
    L.append("Where each defect is caught: **dry_run** (SDP gate, pre-execution), "
             "**runtime** (during execute), or **never** (shipped). Counted at the defect "
             "level across ALL iterations (anti-bypass §9.2 — a gate-caught-then-fixed error "
             "still counts). structural=D1/D4/D5, semantic/silent=D2/D6/D7/D8 (CONTROL), "
             "state=D3/D9. The silent-defect rate (H1 above) is the control, not the headline.\n")
    if et:
        L.append("| arm | group | dry_run (gate) | runtime | never (shipped) |")
        L.append("|---|---|---|---|---|")
        for a, d in et.items():
            for g in ("structural", "semantic", "state"):
                gg = d["by_group"][g]
                L.append(f"| {a} | {g} | {gg['dry_run']} | {gg['runtime']} | {gg['never']} |")
        L.append("\nIteration-level error events per arm: "
                 + "; ".join(f"**{a}** gate={d['gate_error_events']} runtime={d['runtime_error_events']} "
                             f"intercepts={d['dry_run_intercepts']}" for a, d in et.items()))

    L.append("\n## H2 — compute-to-correct: A (execute-only) vs gated loop\n")
    hm = m.get("h2_metric")
    if hm:
        L.append(f"- metric: **`{hm['field']}`** — {hm['label']}")
        L.append(f"- selection: {hm['selection']}\n")
    L.append("Reported BOTH ways (B9): intention-to-treat over all matched cells "
             "(failed runs imputed to total compute spent) AND complete-case "
             "(both arms reached correct) as the pre-specified sensitivity.\n")
    h2 = rep["H2_compute_to_correct"]
    if not h2:
        L.append("_no gated arms in this results set_")
    else:
        L.append("| gated arm | mode | n pairs | median exec-s saved | mean | 95% CI | $ saved | intercept frac |")
        L.append("|---|---|---|---|---|---|---|---|")
        for g_arm, d in h2.items():
            frac = d["intercept_fraction"]
            fracs = "—" if frac is None else f"{frac:.1%} ({d['dry_run_intercepts']}/{d['failing_iterations']})"
            for mode_key in ("itt", "complete_case"):
                s = d[mode_key]
                if s.get("n_pairs", 0) == 0:
                    L.append(f"| {g_arm} | {s['mode']} | 0 | — | — | — | — | {fracs} |")
                    continue
                ci = s["ci95_exec_s_saved"]
                L.append(f"| {g_arm} | {s['mode']} | {s['n_pairs']} | {s['median_exec_s_saved']:.1f} | "
                         f"{s['mean_exec_s_saved']:.1f} | [{ci[0]:.1f}, {ci[1]:.1f}] | "
                         f"${s['total_usd_saved_at_scale']:.2f} | {fracs} |")

    h5 = rep.get("H5_conciseness")
    if h5:
        L.append("\n## H4/H5 — conciseness: declarative (B) vs imperative (A)\n")
        L.append(f"Paired over (task, seed) on the FINAL ACCEPTED program. Contrast "
                 f"**{h5['contrast']}** (locked 2-arm design; A2 withdrawn). Difference is "
                 f"**A − B**, so positive ⇒ the declarative agent wrote LESS. LOC/AST are "
                 f"source-level (substrate-independent). `*_body` excludes the mandatory "
                 f"@dp/def/import scaffolding (the SDP `spark-pipeline.yml` is harness "
                 f"boilerplate and is not counted).\n")
        if h5.get("n_pairs", 0) == 0:
            L.append("_no paired (task,seed) cells where both B and A completed_")
        else:
            L.append("| metric | n pairs | B (declarative) | A (imperative) | "
                     "Δ (A−B) | 95% CI (bootstrap) | % smaller than imperative |")
            L.append("|---|---|---|---|---|---|---|")
            for metric in CONCISENESS_METRICS:
                d = h5["per_metric"].get(metric, {})
                if d.get("n_pairs", 0) == 0:
                    L.append(f"| {metric} | 0 | — | — | — | — | — |")
                    continue
                ci = d["bootstrap_ci95_diff"]
                pct = d["pct_smaller_than_imperative"]
                pcts = "—" if pct is None else f"{pct:.1%}"
                L.append(f"| {metric} | {d['n_pairs']} | {d['mean_declarative']:.1f} | "
                         f"{d['mean_imperative']:.1f} | {d['mean_diff_imp_minus_decl']:+.1f} | "
                         f"[{ci[0]:+.1f}, {ci[1]:+.1f}] | {pcts} |")

    # --- Quarantine (excluded-data appendix, Part B.5) ---------------------
    q = rep.get("quarantine", {"n_quarantined": 0, "cells": [], "by_arm": {}, "by_reason": {}})
    L.append("\n## Quarantine — HARNESS_ERROR cells EXCLUDED from H1–H4 (excluded-data appendix)\n")
    L.append(f"- rows analyzed: **{m.get('n_rows')}** of **{m.get('n_rows_total', m.get('n_rows'))}** "
             f"total; **{q['n_quarantined']}** quarantined (instrument failures, never agent outcomes)")
    if q["n_quarantined"] == 0:
        L.append("- no cells quarantined — every analyzed cell is a genuine agent outcome.\n")
    else:
        L.append(f"- by arm: {q['by_arm']}  |  by reason: {q['by_reason']}\n")
        L.append("| task | seed | arm | exit_class | reason |")
        L.append("|---|---|---|---|---|")
        for c in q["cells"]:
            L.append(f"| {c['task']} | {c['seed']} | {c['arm']} | {c['exit_class']} | {c['reason']} |")
    return "\n".join(L) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description="Safe-agent study statistical analysis (pre-reg §7).")
    ap.add_argument("results", help="path to results.jsonl")
    ap.add_argument("--env", default=None, help="path to results.env.json (for arm gate metadata "
                    "+ the top-level 'backend' that drives H2 metric selection)")
    ap.add_argument("--assume-backend", default=None, choices=sorted(KNOWN_H2_BACKENDS),
                    help="explicit no-env opt-in for the H2 backend {local,live} when the env "
                         "sidecar is unavailable; overrides the sidecar's backend if both are given")
    ap.add_argument("--tasks", default=_DEFAULT_TASKS_LOCK,
                    help="path to TASKS.lock.json for the complexity join (item 2); the "
                         "analysis frame gets a complexity_bin/score column for later H4")
    ap.add_argument("--json-out", default=None, help="write the full report JSON here")
    ap.add_argument("--md-out", default=None, help="write the markdown headline table here")
    ap.add_argument("--quarantine-out", default=None,
                    help="write the SEPARATE quarantine report JSON (excluded HARNESS_ERROR "
                         "cells: task, seed, arm, exit_class, reason) here (Part B.5)")
    args = ap.parse_args(argv)

    arms_meta = None
    backend = None
    if args.env:
        try:
            with open(args.env) as f:
                env = json.load(f)
            arms_meta = env.get("arms")
            backend = env.get("backend")   # drives the explicit H2 metric selection
        except Exception:
            arms_meta = None
    if args.assume_backend:               # explicit opt-in wins over the sidecar
        backend = args.assume_backend

    try:
        rep = build_report(args.results, arms_meta, backend=backend, tasks_path=args.tasks)
    except H2MetricSelectionError as e:
        ap.error(str(e))                  # fail loud (exits non-zero) with an actionable message
    md = render_markdown(rep)
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(rep, f, indent=2)
    if args.quarantine_out:
        with open(args.quarantine_out, "w") as f:
            json.dump(rep.get("quarantine", {"n_quarantined": 0, "cells": []}), f, indent=2)
    if args.md_out:
        with open(args.md_out, "w") as f:
            f.write(md)
    sys.stdout.write(md)


if __name__ == "__main__":
    main()
