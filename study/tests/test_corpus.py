"""Validate the frozen task corpus (TASKS.lock.json) is internally consistent.

No Spark needed -- this is a pure structural check on the lock file + the oracle
registry, run alongside the numeric oracle tests to keep the corpus honest:

  * >= 12 tasks (pre-reg §4 target; D-0 resolved at 15);
  * the published coverage_matrix matches the actual per-class task counts;
  * every D1-D9 class is exhibited by MULTIPLE (>= 5) tasks (no single-task class);
  * every SEMANTIC defect (D2/D6/D7/D8) in a task's scope has a quantifier mapping
    that actually resolves to a real function (single-source quantify.py for
    orders; quantify_ext.py for cdc/payments);
  * task ids are unique; every task has a non-empty per-task prompt;
  * the per-task prompt composed by the runner is IDENTICAL across arms (pre-reg
    §3: the loop is the only manipulated variable, not the task text).
"""
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)


def _find_repo_root():
    # Mirrors harness/runner.py: the original layout put study/ three levels deep;
    # the paper repo puts it two deep. Walk up to the dir holding generators/ or .git.
    env = os.environ.get("STUDY_REPO_ROOT")
    if env:
        return os.path.abspath(env)
    d = HERE
    for _ in range(6):
        d = os.path.dirname(d)
        if os.path.isdir(os.path.join(d, "generators")) or os.path.isdir(os.path.join(d, ".git")):
            return d
    return os.path.normpath(os.path.join(STUDY, "..", ".."))


REPO = _find_repo_root()
sys.path.insert(0, STUDY)

TASKS = json.load(open(os.path.join(STUDY, "config", "TASKS.lock.json")))
SEEDS = json.load(open(os.path.join(STUDY, "config", "SEEDS.lock.json")))
SEMANTIC = {"D2", "D6", "D7", "D8"}
ALL_CLASSES = {f"D{i}" for i in range(1, 10)}


def _battery(fname):
    # The battery lives at <repo>/defect_battery in the paper repo (the SSOT) and at
    # <repo>/experiments/defect_battery in the original working-tree layout.
    for rel in ("defect_battery", os.path.join("experiments", "defect_battery")):
        p = os.path.join(REPO, rel, fname)
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"defect_battery/{fname} not found under {REPO}")


def _load(modname, relpath):
    path = os.path.join(REPO, relpath)
    if not os.path.exists(path):
        path = _battery(os.path.basename(relpath))
    spec = importlib.util.spec_from_file_location(modname, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


QUANT = _load("dbq", "experiments/defect_battery/quantify.py")
QEXT = _load("dbqe", "experiments/defect_battery/quantify_ext.py")
QHC = _load("dbqhc", "experiments/defect_battery/quantify_hc.py")
QUDF = _load("dbqudf", "experiments/defect_battery/quantify_udf.py")

# substrates graded by cross-stage / custom invariants rather than the standard
# output oracle (semantic correctness checked by quantify_hc / quantify_udf).
INVARIANT_SUBSTRATES = {"trades", "clickstream", "emails"}


def _resolve_quant(ref):
    """'quantify.d2' -> QUANT['d2']; 'quantify_ext.cdc_d6' -> QUANT_EXT[...];
    'quantify_hc.hc1_positions' -> QUANT_HC[...]; 'quantify_udf.udf' -> QUANT_UDF[...]."""
    mod, key = ref.split(".", 1)
    if mod == "quantify":
        return QUANT.QUANT.get(key)
    if mod == "quantify_ext":
        return QEXT.QUANT_EXT.get(key)
    if mod == "quantify_hc":
        return QHC.QUANT_HC.get(key)
    if mod == "quantify_udf":
        return QUDF.QUANT_UDF.get(key)
    return None


def test_at_least_12_tasks():
    n = len(TASKS["tasks"])
    assert n >= 12, f"corpus has {n} tasks, pre-reg targets >=12"
    assert TASKS["corpus_status"]["locked_n_tasks"] == n
    assert TASKS["corpus_status"]["below_target"] is False


def test_unique_ids_and_prompts():
    ids = [t["id"] for t in TASKS["tasks"]]
    assert len(ids) == len(set(ids)), "duplicate task ids"
    for t in TASKS["tasks"]:
        assert t.get("prompt", "").strip(), f"task {t['id']} has no prompt"
        assert t.get("defects_in_scope"), f"task {t['id']} has no defects_in_scope"
        for d in t["defects_in_scope"]:
            assert d in ALL_CLASSES, f"task {t['id']} bad defect {d}"


def test_coverage_matrix_matches_actual():
    actual = {c: 0 for c in ALL_CLASSES}
    for t in TASKS["tasks"]:
        for d in t["defects_in_scope"]:
            actual[d] += 1
    published = TASKS["coverage_matrix"]
    for c in ALL_CLASSES:
        assert published.get(c) == actual[c], f"{c}: published {published.get(c)} != actual {actual[c]}"


def test_every_class_multiple_tasks():
    actual = {c: 0 for c in ALL_CLASSES}
    for t in TASKS["tasks"]:
        for d in t["defects_in_scope"]:
            actual[d] += 1
    for c in ALL_CLASSES:
        assert actual[c] >= 5, f"class {c} only in {actual[c]} tasks (want >= 5; multiple-task requirement)"
    # structural and semantic/state families each well represented
    assert all(actual[c] >= 5 for c in ("D1", "D4", "D5")), "structural under-represented"
    assert all(actual[c] >= 5 for c in ("D2", "D3", "D6", "D7", "D8", "D9")), "semantic/state under-represented"


def test_semantic_defects_have_resolvable_quantifier():
    """OUTPUT-ORACLE-graded tasks must map each in-scope semantic defect to a
    resolvable quantifier. INVARIANT-graded tasks (HC / UDF) grade their semantic
    correctness via cross-stage invariants instead -- checked separately below."""
    for t in TASKS["tasks"]:
        if t.get("graded_by") == "invariants":
            continue
        oracles = t.get("oracles", {})
        for d in t["defects_in_scope"]:
            if d in SEMANTIC:
                ref = oracles.get(d)
                assert ref, f"task {t['id']} scopes semantic {d} but has no oracle mapping"
                fn = _resolve_quant(ref)
                assert callable(fn), f"task {t['id']} oracle {ref} for {d} does not resolve to a function"


def test_semantic_tasks_have_output_contract():
    """Every OUTPUT-ORACLE-graded task with a semantic defect must declare an
    output_contract whose columns let the LIVE output oracle grade it (B1)."""
    need = {"D2": "date_col", "D7": "date_col", "D8": "revenue_col", "D6": "key_col"}
    for t in TASKS["tasks"]:
        if t.get("graded_by") == "invariants":
            continue
        sem = [d for d in t["defects_in_scope"] if d in SEMANTIC]
        if not sem:
            continue
        c = t.get("output_contract")
        assert c, f"task {t['id']} has semantic defects {sem} but no output_contract"
        assert c.get("table"), f"task {t['id']} output_contract has no table"
        for d in sem:
            col = need[d]
            assert c.get(col), f"task {t['id']} scopes {d} but contract lacks {col}"
        assert c.get("substrate") in ("orders", "cdc", "payments"), t["id"]


def test_graded_by_is_declared_and_consistent():
    """Every task declares graded_by in {output_oracle, invariants}; invariant
    substrates are graded by invariants and vice-versa is allowed for elevated
    same-substrate tasks (which keep an output oracle AND add invariants)."""
    for t in TASKS["tasks"]:
        gb = t.get("graded_by")
        assert gb in ("output_oracle", "invariants"), f"{t['id']} bad graded_by {gb!r}"
        if t.get("output_contract", {}).get("substrate") in INVARIANT_SUBSTRATES:
            assert gb == "invariants", f"{t['id']} on invariant substrate must be invariant-graded"


def test_invariant_oracles_resolve():
    """Every declared invariant (HC tasks, UDF task, and the elevated p2/p10/p13/
    p14 cross-stage reconciliations) references a resolvable quantify_hc /
    quantify_udf / quantify_ext function -- so the corpus cannot claim an invariant
    that has no ground-truth implementation."""
    seen = 0
    for t in TASKS["tasks"]:
        for inv in t.get("invariants", []):
            ref = inv.get("oracle")
            assert ref, f"{t['id']} invariant {inv.get('name')} has no oracle"
            assert callable(_resolve_quant(ref)), (
                f"{t['id']} invariant {inv['name']} oracle {ref} unresolved")
            seen += 1
    assert seen >= 10, f"expected many invariants across HC + elevated tasks, saw {seen}"


def test_invariant_graded_tasks_have_invariants():
    """HC-1, HC-2, and the UDF task must each carry at least one invariant."""
    for tid in ("HC1_fx_trade_ledger", "HC2_session_funnel", "new_udf_classifier"):
        t = next(x for x in TASKS["tasks"] if x["id"] == tid)
        assert t.get("graded_by") == "invariants", tid
        assert t.get("invariants"), f"{tid} has no invariants"


def test_substrate_quantifiers_resolve():
    for name, sub in TASKS["substrates"].items():
        for d, ref in sub.get("quantifiers", {}).items():
            assert callable(_resolve_quant(ref)), f"substrate {name} quantifier {ref} unresolved"


def test_per_task_prompt_is_arm_independent():
    from harness.runner import compose_task_prompt
    from harness.arm_manifest import load_arms
    arms = load_arms(os.path.join(STUDY, "arms"))
    preamble = open(os.path.join(STUDY, "prompts", "task_prompt.md")).read()
    for t in TASKS["tasks"]:
        prompts = {a: compose_task_prompt(preamble, t) for a in arms}
        assert len(set(prompts.values())) == 1, (
            f"task {t['id']} composes a different prompt per arm -- pre-reg §3 violated")


def test_seeds_consistent():
    seeds = SEEDS["seeds"]
    assert len(seeds) == len(set(seeds)), "duplicate seeds"
    assert all(isinstance(s, int) for s in seeds), "seeds must be integers"
    # v1.1.0-power appended 2 seeds (10 pilot -> 12 power) per DEVIATIONS D-SEEDS-POWER:
    # the lock now carries the full power set; pilot_n records the historical 10-seed
    # pilot subset separately (intentionally NOT len(seeds)).
    assert SEEDS["power_n"] == len(seeds), "seed list must equal power_n (12)"
    assert SEEDS["pilot_n"] == 10, "pilot used the first 10 seeds (pre-reg §6 stage 2)"


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
