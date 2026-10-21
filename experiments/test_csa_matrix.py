"""CSA matrix: 180 cells, 12-cell smoke, canaries, SATURATED fail-fast."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import make_csa_matrix  # noqa: E402
from csa_profiles import BROKERS, FAULTS, PROFILES, REPS, SCHEMES, SMOKE_REPS  # noqa: E402


def test_profiles_match_the_brief_and_divide_evenly():
    assert PROFILES["whiteboard"] == {"clients": 20, "rate": 5.0, "rooms": 25, "load": 2500}
    assert PROFILES["quiz"] == {"clients": 30, "rate": 0.2, "rooms": 10, "load": 60}
    assert PROFILES["class-chat"] == {"clients": 50, "rate": 0.5, "rooms": 10, "load": 250}
    for profile in PROFILES.values():
        assert profile["rooms"] * profile["clients"] * profile["rate"] == profile["load"]
    assert SCHEMES == ("yjs", "sharedb")
    assert BROKERS == ("pubsub", "streams", "kafka-tuned")
    assert len(SCHEMES) * len(BROKERS) == 6
    assert [f["disconnects_per_min"] for f in FAULTS] == [0, 1]
    assert FAULTS[1]["gateway_kill"] == 1


def test_full_matrix_is_180_and_smoke_is_12():
    full = make_csa_matrix.build("full")
    make_csa_matrix.check_plan(full, "full")
    assert len(full) == 180
    assert len(full) == len(SCHEMES) * len(BROKERS) * len(PROFILES) * len(FAULTS) * REPS
    smoke = make_csa_matrix.build("smoke")
    make_csa_matrix.check_plan(smoke, "smoke")
    assert len(smoke) == 12
    assert len(smoke) == len(SCHEMES) * len(BROKERS) * len(FAULTS) * SMOKE_REPS
    assert {r["stage"] for r in smoke} == {"whiteboard"}
    assert {r["rep"] for r in smoke} == {0}


def test_canary_is_the_high_fault_cell():
    rows = make_csa_matrix.build("full")
    first = {}
    for item in rows:
        first.setdefault((item["stage"], item["scheme"], item["broker"]), item)
    assert len(first) == 18
    assert all(cell["disconnects_per_min"] == 1 and cell["gateway_kill"] == 1 for cell in first.values())
    assert all(cell["rep"] == 0 for cell in first.values())


def test_saturated_token_is_mandatory(tmp_path):
    make_csa_matrix.require_saturated_flag(Path(__file__).resolve().parent / "run_matrix.sh")
    missing = tmp_path / "run_matrix.sh"
    missing.write_text("echo no flag\n")
    with pytest.raises(ValueError, match="SATURATED"):
        make_csa_matrix.require_saturated_flag(missing)


def test_run_csa_fail_fast_is_on():
    text = (Path(__file__).resolve().parent / "run_csa.sh").read_text()
    assert "VALIDITY_FAIL_FAST=1" in text
    assert "make_csa_matrix.py" in text


def test_write_puts_scheme_last(tmp_path):
    rows = make_csa_matrix.build("smoke")
    out = tmp_path / "csa.csv"
    make_csa_matrix.write_matrix(rows, str(out))
    header = out.read_text().splitlines()[0].split(",")
    assert header[-1] == "scheme"
    assert "sharedb" in out.read_text()
    assert "yjs" in out.read_text()
