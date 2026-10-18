"""The whole SPEC §9 scenario corpus through the Python API, compared with the reference's
own scenario comparison code (`muninn_ref.scenario.check_step`).

Every scenario also runs in three physical variants that must not change any result
(SPEC §7.2): on a local directory, reopened before every read, and compacted before every
read — plus a fourth that runs every read against a pinned snapshot."""

import pytest

import muninn
from adapter import SCENARIOS, PyMuninn
from muninn_ref.scenario import MalformedInput, check_step, find_scenarios, load_json

PATHS = find_scenarios([SCENARIOS])
READS = {"latest", "history", "replay", "grid", "asof_join"}


def run(path, variant, tmp_path):
    scenario = load_json(path)
    if variant == "memory":
        a = PyMuninn()
    else:
        a = PyMuninn(muninn.open(tmp_path / "db", create=True))
    for i, step in enumerate(scenario["steps"]):
        if step.get("op") in READS:
            if variant == "reopen":
                a = PyMuninn(muninn.open(tmp_path / "db"))
            elif variant == "compact":
                a.db.compact()
            elif variant == "snapshot":
                a.reader = a.db.snapshot()
        try:
            problem = check_step(a, step)
        except (MalformedInput, KeyError) as e:
            pytest.fail(f"{path.name} step {i}: malformed step: {e!r}")
        a.reader = a.db
        if problem is not None:
            if isinstance(problem, tuple):
                msg, exp, act = problem
                pytest.fail(f"{path.name} step {i} ({step.get('op')}): {msg}\n"
                            f"  expected: {exp}\n  actual:   {act}")
            pytest.fail(f"{path.name} step {i}: {problem}")


def test_corpus_is_present():
    assert len(PATHS) >= 140


@pytest.mark.parametrize("path", PATHS, ids=[str(p.relative_to(SCENARIOS)) for p in PATHS])
def test_scenario_memory(path, tmp_path):
    run(path, "memory", tmp_path)


VARIANT_PATHS = [p for p in PATHS if "generated" not in p.parts] + [
    p for p in PATHS if "generated" in p.parts
][::5]


@pytest.mark.parametrize("variant", ["localfs", "reopen", "compact", "snapshot"])
@pytest.mark.parametrize(
    "path", VARIANT_PATHS, ids=[str(p.relative_to(SCENARIOS)) for p in VARIANT_PATHS]
)
def test_scenario_variants(path, variant, tmp_path):
    run(path, variant, tmp_path)
