"""MMC matrices: own seed, E1 saturation, E2 windows, 6-cell smoke."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import make_matrix  # noqa: E402
import make_mmc_matrix as mmc  # noqa: E402


def test_mmc_seed_is_not_the_factorial_seed():
    assert mmc.MMC_SEED != make_matrix.SEED


def test_e1_is_four_transports_eight_loads_three_reps():
    rows = mmc.e1_rows()
    assert len(rows) == 4 * 8 * 3
    assert {r["broker"] for r in rows} == set(mmc.TRANSPORTS)
    assert {r["load"] for r in rows} == set(mmc.E1_LOADS)
    assert {r["rep"] for r in rows} == {0, 1, 2}
    assert {r["duration_s"] for r in rows} == {240}
    assert all(r["disconnects_per_min"] == 0 and r["gateway_kill"] == 0 for r in rows)
    assert all(r["stage"] == "B" for r in rows)
    first = {}
    for item in rows:
        first.setdefault(item["broker"], item)
    assert first["kafka"]["load"] == 6000


def test_e2_windows_and_streams_only_at_6000():
    rows = mmc.e2_rows()
    assert len(rows) == 60
    kafka = [r for r in rows if r["broker"] == "kafka"]
    tuned = [r for r in rows if r["broker"] == "kafka-tuned"]
    streams = [r for r in rows if r["broker"] == "streams"]
    assert len(kafka) == 2 * 4 * 3
    assert len(tuned) == 2 * 4 * 3
    assert len(streams) == 1 * 4 * 3
    assert {r["load"] for r in kafka} == {5000, 6000}
    assert {r["load"] for r in tuned} == {5000, 6000}
    assert {r["load"] for r in streams} == {6000}
    assert {r["duration_s"] for r in rows} == {60, 120, 240, 480}
    assert all(r["disconnects_per_min"] == 0 and r["gateway_kill"] == 0 for r in rows)
    assert mmc.e2_rows() == rows


def test_mmc_smoke_is_six_cells():
    rows = mmc.smoke_rows()
    assert len(rows) == 6
    assert {r["broker"] for r in rows} == set(mmc.TRANSPORTS)
    assert sum(1 for r in rows if r["duration_s"] == 240) == 5
    assert sum(1 for r in rows if r["duration_s"] == 60) == 1
    assert any(r["broker"] == "kafka" and r["load"] == 6000 for r in rows)
    assert any(r["broker"] == "streams" and r["load"] == 6000 and r["duration_s"] == 60 for r in rows)


def test_duration_is_a_matrix_column_and_run_id_suffix(tmp_path: Path):
    rows = mmc.smoke_rows()
    out = tmp_path / "matrix.csv"
    make_matrix.write_matrix(rows, str(out))
    text = out.read_text()
    assert text.splitlines()[0].endswith("duration_s,run_id")
    assert "-T240" in text
    assert "-T60" in text
    parsed = make_matrix.parse_cell_id("004-B-kafka-L6000-nu0-k0-r0-T240")
    assert parsed["duration_s"] == 240
    assert parsed["load"] == 6000
    assert parsed["run_id"].endswith("-T240")
