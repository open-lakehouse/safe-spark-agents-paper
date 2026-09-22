"""H5 conciseness instrumentation: program-size metrics + the B-vs-A2 contrast.

Pins, on a KNOWN declarative sample and a KNOWN imperative sample, the four
conciseness numbers (`final_program_loc` / `ast_node_count`, each raw AND
transform-body-only) computed by `harness/program_metrics.py`; the None behaviour on
an incomplete / unparseable program; that a result row carrying the new fields
validates against BOTH the hand validator and the published `results_schema.json`
(arm `enum` now includes A2); and that `analysis/analyze.py` runs the paired B-vs-A2
conciseness contrast on a fixture and reports the declarative arm as smaller.
"""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

import jsonschema  # noqa: E402

from harness import program_metrics as pm  # noqa: E402
from harness.schema import ResultRow, validate_row  # noqa: E402

import importlib.util  # noqa: E402
_spec = importlib.util.spec_from_file_location(
    "analyze", os.path.join(STUDY, "analysis", "analyze.py"))
analyze = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analyze)

PUBLISHED = os.path.join(STUDY, "config", "results_schema.json")

# --- the two KNOWN samples (line-numbered in the comments for the LOC hand-check) ---
# Declarative (SDP) — the @dp wrapper + def are mandatory scaffolding the agent MUST
# write; the body is just the 3 transform statements.
DECL = (
    "from pyspark import pipelines as dp\n"               # 1  import        (scaffold)
    "from pyspark.sql import functions as F\n"            # 2  import        (scaffold)
    "\n"                                                  # 3  blank
    "# clean + dedup + enrich orders\n"                   # 4  comment
    "@dp.materialized_view(name=\"gold_orders\")\n"       # 5  decorator     (scaffold)
    "def gold_orders():\n"                                # 6  def header    (scaffold)
    "    df = spark.read.json(spark.conf.get(\"input_path\"))\n"  # 7  body
    "    deduped = df.dropDuplicates([\"order_id\"])\n"   # 8  body
    "    return deduped.withColumn(\"amount\", F.col(\"amount\").cast(\"double\"))\n"  # 9 body
)

# Imperative — the agent hand-rolls the SparkSession + input/output plumbing; that IS
# real decision surface (the declarative agent never writes it), so it counts as body.
IMP = (
    "import os\n"                                                       # 1 import   (scaffold)
    "from pyspark.sql import SparkSession, functions as F\n"           # 2 import   (scaffold)
    "\n"                                                               # 3 blank
    "spark = SparkSession.builder.getOrCreate()\n"                     # 4 body
    "df = spark.read.json(os.environ[\"AGENT_INPUT_PATH\"])\n"         # 5 body
    "deduped = df.dropDuplicates([\"order_id\"])\n"                    # 6 body
    "out = deduped.withColumn(\"amount\", F.col(\"amount\").cast(\"double\"))\n"  # 7 body
    "out.write.mode(\"overwrite\").parquet(os.environ[\"AGENT_OUTPUT_PATH\"])\n"  # 8 body
    "print(\"Run is COMPLETED\")\n"                                    # 9 body
)

# Expected counts. LOC is fully hand-derivable from the line annotations above; the
# AST-node counts are the deterministic stdlib `ast` totals for these exact samples
# (every walk node incl. Module/Load/Store/operator nodes) and act as regression pins.
# NOTE the `@dp.materialized_view(name="gold_orders")` decorator carries an argument
# (the agent-chosen name) -> it is LOGIC, not scaffolding, so its line/nodes stay in
# the body-only counts (cross-review fix): DECL body LOC = 4 (not 3), keeping line 5.
EXPECT_DECL = {"final_program_loc": 7, "final_program_loc_body": 4,
               "ast_node_count": 60, "ast_node_count_body": 54}
EXPECT_IMP = {"final_program_loc": 8, "final_program_loc_body": 6,
              "ast_node_count": 87, "ast_node_count_body": 82}

# --- decorator scaffolding-vs-logic samples (the cross-review cases) ----------------
# (a) and (b) share a BYTE-IDENTICAL def body, so the only difference is the
# decorator: (a) a BARE structural wrapper -> stripped; (b) the same wrapper PLUS a
# logic-bearing expectation -> the expectation is KEPT. The body delta therefore
# isolates exactly the expectation's lines/nodes.
BARE_DECORATOR = (
    "@dp.table\n"                                          # 1 bare wrapper (scaffold)
    "def t():\n"                                           # 2 def header   (scaffold)
    "    return spark.read.json(p)\n"                      # 3 body
)
LOGIC_DECORATOR = (
    "@dp.table\n"                                          # 1 bare wrapper (scaffold)
    "@dp.expect(\"valid_amount\", \"amount > 0\")\n"       # 2 LOGIC (kept in body)
    "def t():\n"                                           # 3 def header   (scaffold)
    "    return spark.read.json(p)\n"                      # 4 body
)
# (c) the IMPERATIVE equivalent of the same quality constraint, inline.
IMPERATIVE_QUALITY = (
    "df = spark.read.json(p)\n"                            # 1 body
    "clean = df.filter(\"amount > 0\")\n"                  # 2 body (the quality logic)
    "clean.write.parquet(out)\n"                           # 3 body
)


def test_declarative_sample_counts():
    assert pm.program_metrics(DECL) == EXPECT_DECL


def test_imperative_sample_counts():
    assert pm.program_metrics(IMP) == EXPECT_IMP


def test_body_only_strips_scaffolding_not_logic():
    """Body-only LOC = raw minus exactly the decision-free scaffolding lines: DECL
    loses 2 imports + 1 def header (the logic decorator is KEPT) -> 7→4; IMP loses 2
    imports only -> 8→6. The declarative BODY is still smaller than the imperative
    body on every metric — the H5 headline (the imperative session/IO plumbing is its
    own surface) — but now without silently dropping declarative decorator logic."""
    d, i = pm.program_metrics(DECL), pm.program_metrics(IMP)
    assert d["final_program_loc"] - d["final_program_loc_body"] == 3   # 2 imports + 1 def (logic dec kept)
    assert i["final_program_loc"] - i["final_program_loc_body"] == 2   # 2 imports
    assert d["final_program_loc_body"] < i["final_program_loc_body"]
    assert d["ast_node_count_body"] < i["ast_node_count_body"]
    for k, v in d.items():
        assert v is None or v >= 0
        if k.endswith("_body"):
            assert v <= pm.program_metrics(DECL)[k.replace("_body", "")]


def test_bare_decorator_is_stripped_from_body():
    """(a) A bare `@dp.table` (no args) is decision-free scaffolding: stripped from
    body-only along with the def header, leaving just the transform statement."""
    m = pm.program_metrics(BARE_DECORATOR)
    assert m["final_program_loc"] == 3
    assert m["final_program_loc_body"] == 1                # only the `return ...` line
    # the bare decorator + def header contribute no body AST nodes
    assert m["ast_node_count_body"] < m["ast_node_count"]


def test_logic_bearing_decorator_is_counted_in_body():
    """(b) A `@dp.expect(...)` carries an agent DECISION (a data-quality expectation):
    it MUST stay in body-only LOC and AST. Pinned against the bare-only program: the
    only added authored line is the expectation, and it shows up in the body."""
    logic = pm.program_metrics(LOGIC_DECORATOR)
    bare = pm.program_metrics(BARE_DECORATOR)
    # body LOC = the kept expectation line + the return line
    assert logic["final_program_loc_body"] == 2
    # the expectation is genuinely COUNTED, not stripped: removing it (bare-only) drops
    # both a body LOC and body AST nodes.
    assert logic["final_program_loc_body"] > bare["final_program_loc_body"]
    assert logic["ast_node_count_body"] > bare["ast_node_count_body"]
    # and the expectation's own subtree (Call + 2 string args) is in the body delta
    assert logic["ast_node_count_body"] - bare["ast_node_count_body"] >= 3


def test_decorator_logic_symmetry_declarative_vs_imperative():
    """(c) Symmetry — the SAME predicate, no paradigm special-casing: a declarative
    quality constraint expressed via `@dp.expect(...)` and the equivalent imperative
    `.filter("amount > 0")` are BOTH counted in their body-only metrics. Neither arm's
    quality logic vanishes, so the body-only metric does not favor declarative."""
    decl = pm.program_metrics(LOGIC_DECORATOR)
    imp = pm.program_metrics(IMPERATIVE_QUALITY)
    # imperative: no scaffolding at all -> body == raw, the quality filter counted
    assert imp["final_program_loc_body"] == imp["final_program_loc"] == 3
    # declarative: the expectation survives into body-only (it is not stripped)
    assert decl["final_program_loc_body"] == 2
    # both arms retain a non-trivial body AST that includes their quality logic
    assert decl["ast_node_count_body"] > 5 and imp["ast_node_count_body"] > 5


def test_none_and_empty_program_is_all_none():
    allnone = {k: None for k in
               ("final_program_loc", "final_program_loc_body",
                "ast_node_count", "ast_node_count_body")}
    assert pm.program_metrics(None) == allnone
    assert pm.program_metrics("") == allnone
    assert pm.program_metrics("   \n\n  # just a comment\n") == allnone or \
        pm.program_metrics("   \n\n  # just a comment\n")["final_program_loc"] == 0


def test_syntax_error_keeps_raw_loc_drops_ast():
    """An unparseable program still has a meaningful raw LOC; everything AST-derived
    (and body LOC, which needs the parse) is None — never a crash."""
    m = pm.program_metrics("def f(:\n    pass\n")
    assert m["final_program_loc"] == 2
    assert m["final_program_loc_body"] is None
    assert m["ast_node_count"] is None
    assert m["ast_node_count_body"] is None


# ---------------------------------------------------------------------------
# schema: a row carrying the new fields validates against the published contract
# ---------------------------------------------------------------------------
def _published():
    with open(PUBLISHED) as f:
        return json.load(f)


def test_published_schema_arm_enum_includes_a2():
    assert "A2" in _published()["properties"]["arm"]["enum"]


def test_row_with_conciseness_fields_validates():
    cm = pm.program_metrics(DECL)
    row = ResultRow(
        run_id="p1__A2__seed42", task="p1", arm="A2", seed=42,
        spark_version="4.1.0", image_digest="uncontainerized", git_sha="abc",
        base_model_id="claude-opus-4-8", executor_config={"instances": 4},
        silent_defect=False, defect_classes=[], detection_stage="n/a",
        iterations=2, wall_s=1.0, executor_seconds=None, usd=0.0,
        exit_class="completed", task_success=True, reached_correct=True,
        executor_seconds_wallclock=0.0,
        final_program=DECL,
        final_program_loc=cm["final_program_loc"],
        final_program_loc_body=cm["final_program_loc_body"],
        ast_node_count=cm["ast_node_count"],
        ast_node_count_body=cm["ast_node_count_body"],
    )
    d = json.loads(row.to_json())
    assert d["arm"] == "A2" and d["final_program_loc"] == 7
    assert validate_row(d) == [], validate_row(d)
    jsonschema.validate(d, _published())   # additionalProperties:false -> new fields must be declared


def test_row_with_null_conciseness_fields_validates():
    """An incomplete run carries null conciseness fields (no accepted program)."""
    row = ResultRow(
        run_id="p1__B__seed7", task="p1", arm="B", seed=7,
        spark_version="4.1.0", image_digest="uncontainerized", git_sha="abc",
        base_model_id="claude-opus-4-8", executor_config={"instances": 4},
        silent_defect=False, defect_classes=[], detection_stage="n/a",
        iterations=12, wall_s=1.0, executor_seconds=None, usd=0.0,
        exit_class="max_iterations", executor_seconds_wallclock=0.0,
    )
    d = json.loads(row.to_json())
    assert d["final_program"] is None and d["final_program_loc"] is None
    assert validate_row(d) == [], validate_row(d)
    jsonschema.validate(d, _published())


# ---------------------------------------------------------------------------
# analysis: the paired B-vs-A2 conciseness contrast runs and finds B smaller
# ---------------------------------------------------------------------------
def _row(task, arm, seed, loc, loc_body, ast_n, ast_body, completed=True):
    return {
        "run_id": f"{task}__{arm}__seed{seed}", "task": task, "arm": arm, "seed": seed,
        "spark_version": "x", "image_digest": "x", "git_sha": "x",
        "base_model_id": "claude-opus-4-8", "executor_config": {},
        "silent_defect": False, "defect_classes": [], "detection_stage": "n/a",
        "iterations": 1, "wall_s": 1.0, "executor_seconds": None, "usd": 0.0,
        "exit_class": "completed" if completed else "max_iterations",
        "executor_seconds_wallclock": 0.0,
        "final_program_loc": loc, "final_program_loc_body": loc_body,
        "ast_node_count": ast_n, "ast_node_count_body": ast_body,
    }


def _fixture_rows():
    rows = []
    # two tasks, two seeds; B (declarative) consistently smaller than A (imperative).
    # Locked 2-arm design (paper §6.1): the conciseness contrast is B-vs-A (A2 withdrawn).
    for task in ("t1", "t2"):
        for seed in (1, 2):
            rows.append(_row(task, "B", seed, 70, 30, 600, 330))
            rows.append(_row(task, "A", seed, 175, 120, 1500, 1450))
    # an incomplete A cell with null metrics -> dropped from the pairing
    rows.append(_row("t3", "B", 1, 70, 30, 600, 330))
    rows.append(_row("t3", "A", 1, None, None, None, None, completed=False))
    return rows


def test_conciseness_contrast_pairs_and_finds_B_smaller():
    idx = analyze.cell_index(_fixture_rows())
    h5 = analyze.conciseness_analysis(idx)
    assert h5["contrast"] == "B-vs-A"
    loc = h5["per_metric"]["final_program_loc"]
    # 4 complete paired cells; t3's incomplete A is dropped
    assert loc["n_pairs"] == 4
    assert loc["mean_declarative"] == 70.0
    assert loc["mean_imperative"] == 175.0
    assert loc["mean_diff_imp_minus_decl"] == 105.0      # positive => B smaller
    assert loc["bootstrap_ci95_diff"] == [105.0, 105.0]  # zero variance -> tight CI
    assert abs(loc["pct_smaller_than_imperative"] - 105.0 / 175.0) < 1e-9
    # body-only metric is present and also shows B smaller
    assert h5["per_metric"]["final_program_loc_body"]["mean_diff_imp_minus_decl"] == 90.0


def test_build_report_runs_with_conciseness_on_a_results_file():
    """End-to-end: analyze.build_report + render_markdown on a fixture results.jsonl
    emits the H5 section without error."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "results.jsonl")
        with open(path, "w") as f:
            for r in _fixture_rows():
                f.write(json.dumps(r) + "\n")
        # H2 metric selection now requires an explicit known backend (#37); the
        # fixture rows carry the local-style wall-clock field, so analyze as local.
        rep = analyze.build_report(path, None, backend="local")
        md = analyze.render_markdown(rep)
    assert "H5_conciseness" in rep
    assert rep["H5_conciseness"]["per_metric"]["ast_node_count"]["n_pairs"] == 4
    assert "H5 — conciseness" in md


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback; failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
