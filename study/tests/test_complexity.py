"""Complexity-rubric presence + consistency (corpus v3 §2/§3).

Every task in TASKS.lock.json must carry an a-priori `complexity_score`, its
`complexity_bin`, and the per-axis `complexity_axes` breakdown. The stored score
is recomputed from the axes through the ONE rubric module (harness.complexity) so
it can never drift from the published weights -- the same single-source discipline
the semantic oracles use against quantify.py.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import complexity as cx  # noqa: E402

TASKS = json.load(open(os.path.join(STUDY, "config", "TASKS.lock.json")))


def test_rubric_weights_and_bins_are_stable():
    # the pre-registered rubric: 8 axes, the documented weights, the documented bins.
    assert tuple(cx.AXIS_KEYS) == ("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8")
    assert cx.WEIGHTS == {"A1": 2, "A2": 3, "A3": 2, "A4": 1, "A5": 2, "A6": 3, "A7": 3, "A8": 1}
    assert cx.bin_of(15) == "Low" and cx.bin_of(16) == "Med"
    assert cx.bin_of(30) == "Med" and cx.bin_of(31) == "High"
    assert cx.max_score() == 51


def test_every_task_has_complexity_block():
    for t in TASKS["tasks"]:
        assert isinstance(t.get("complexity_score"), int), f"{t['id']} has no integer complexity_score"
        assert t.get("complexity_bin") in ("Low", "Med", "High"), f"{t['id']} bad complexity_bin"
        axes = t.get("complexity_axes")
        assert isinstance(axes, dict), f"{t['id']} has no complexity_axes"
        assert set(axes) == set(cx.AXIS_KEYS), f"{t['id']} axes keys {sorted(axes)}"
        for k, v in axes.items():
            assert isinstance(v, int) and 0 <= v <= 3, f"{t['id']} axis {k}={v!r} out of 0..3"


def test_stored_score_matches_rubric():
    for t in TASKS["tasks"]:
        recomputed = cx.score(t["complexity_axes"])
        assert recomputed == t["complexity_score"], (
            f"{t['id']} stored score {t['complexity_score']} != rubric {recomputed}")
        assert cx.bin_of(recomputed) == t["complexity_bin"], (
            f"{t['id']} stored bin {t['complexity_bin']} != {cx.bin_of(recomputed)}")


def test_distribution_is_published():
    """The corpus carries an aggressive complexity gradient: the High bin must be
    populated (>= 4 here at commit time; target >= 7 once new tasks land)."""
    from collections import Counter
    dist = Counter(t["complexity_bin"] for t in TASKS["tasks"])
    assert dist["High"] >= 4, f"High bin under-populated: {dict(dist)}"
    assert dist["Low"] >= 1 and dist["Med"] >= 1, f"gradient collapsed: {dict(dist)}"


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
