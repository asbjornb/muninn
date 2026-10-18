"""Every scenario in spec/scenarios/ (hand-written and generated) passes on the reference."""

from pathlib import Path

import pytest

from muninn_ref.scenario import find_scenarios, run_scenario

SCENARIOS = Path(__file__).resolve().parents[2] / "scenarios"
PATHS = find_scenarios([SCENARIOS])


def test_corpus_is_present():
    assert len([p for p in PATHS if "generated" not in p.parts]) >= 40
    assert len([p for p in PATHS if "generated" in p.parts]) >= 100


@pytest.mark.parametrize("path", PATHS, ids=[str(p.relative_to(SCENARIOS)) for p in PATHS])
def test_scenario(path):
    failures = run_scenario(path)
    assert not failures, "\n".join(f.render() for f in failures)


def test_generator_is_deterministic_and_matches_corpus():
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import generate

    for seed in (1, 2, 77, 150):
        on_disk = (SCENARIOS / "generated" / f"{seed:04d}-random.json").read_text(encoding="utf-8")
        assert generate.dumps(generate.generate(seed)) == on_disk
