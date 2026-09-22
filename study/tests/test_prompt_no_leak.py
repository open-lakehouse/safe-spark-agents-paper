"""Prompt-no-leak guard (corpus v3 §1).

The v3 corpus reframes every task `prompt` as a stakeholder TICKET: state the
business *symptom* so the requirement is implied, pin the deterministic output
contract at the bottom, and NEVER hand the agent the fix, the Spark API, or the
defect name. Spoon-feeding the recipe shrinks the very silent-defect gap the
study measures (it hands both arms the answer), so this guard fails the build if
any prompt — or the shared preamble — leaks a how-to token, an API symbol, or a
defect-class name.

Symptoms ARE allowed (duplicate, late, missing, malformed, foreign currency,
wrong totals, double-counting…). What is banned is the *solution* vocabulary.

The check runs over BOTH the raw per-task `prompt` and the full prompt the runner
composes (shared preamble + brief), so a leak cannot hide in the preamble.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

TASKS = json.load(open(os.path.join(STUDY, "config", "TASKS.lock.json")))
PREAMBLE = open(os.path.join(STUDY, "prompts", "task_prompt.md")).read()

# Single source of truth for the banned vocabulary + checker (importable from the
# `harness` package so every test uses the same one -- see harness/prompt_guard.py).
from harness.prompt_guard import leaks as _leaks  # noqa: E402


def test_per_task_prompt_no_leak():
    for t in TASKS["tasks"]:
        hits = _leaks(t.get("prompt", ""))
        assert not hits, f"task {t['id']} prompt leaks banned tokens {hits}"


def test_composed_prompt_no_leak():
    from harness.runner import compose_task_prompt
    for t in TASKS["tasks"]:
        composed = compose_task_prompt(PREAMBLE, t)
        hits = _leaks(composed)
        assert not hits, f"task {t['id']} composed prompt leaks banned tokens {hits}"


def test_preamble_no_leak():
    hits = _leaks(PREAMBLE)
    assert not hits, f"shared preamble leaks banned tokens {hits}"


def test_every_prompt_pins_output_contract():
    """The deterministic output contract must be pinned at the bottom of the brief
    (ticket-style §1): the agent gets the WHAT (tables/columns), never the HOW."""
    for t in TASKS["tasks"]:
        p = t.get("prompt", "")
        assert "output contract" in p.lower(), f"task {t['id']} prompt has no pinned output contract"


def test_symptoms_are_still_present():
    """Sanity: the reframe must keep the prompts SUBSTANTIVE (a real business
    symptom), not strip them to a bare contract."""
    for t in TASKS["tasks"]:
        head = t.get("prompt", "").split("Output contract")[0]
        assert len(head.split()) >= 20, f"task {t['id']} brief is too thin to imply a requirement"


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
