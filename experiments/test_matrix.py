"""Matrix modes used by bench-up: smoke, kafka-rerun, full."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import make_matrix  # noqa: E402


def _has(rows, stage, broker, load, nu=None):
    return any(
        r["stage"] == stage and r["broker"] == broker and r["load"] == load and (nu is None or r["disconnects_per_min"] == nu)
        for r in rows
    )


def test_scheme_stays_yjs_so_the_factorial_is_not_doubled():
    rows = make_matrix.smoke_rows()
    assert len(rows) == 17
    assert {r["scheme"] for r in rows} == {"yjs"}
    assert make_matrix.SCHEMES == ("yjs",)
    full = make_matrix.factorial_rows(1)
    assert {r["scheme"] for r in full} == {"yjs"}


def test_write_matrix_appends_scheme(tmp_path):
    rows = make_matrix.smoke_rows()
    out = tmp_path / "m.csv"
    make_matrix.write_matrix(rows, str(out))
    header = out.read_text().splitlines()[0].split(",")
    assert header[-1] == "scheme"
    assert "run_id" in header and header.index("run_id") < header.index("scheme")


def test_smoke_covers_the_risky_configs():
    rows = make_matrix.smoke_rows()
    assert len(rows) == 17
    assert {r["stage"] for r in rows} == {"A", "B", "C"}
    assert _has(rows, "B", "kafka", 5000)
    assert _has(rows, "B", "kafka-acks", 5000)
    assert _has(rows, "C", "kafka-tuned", 2500, 0.01)
    assert _has(rows, "C", "kafka-tuned", 2500, 1)
    assert _has(rows, "C", "kafka", 2500)
    assert _has(rows, "C", "kafka", 5000)
    assert _has(rows, "C", "kafka-acks", 2500)
    assert _has(rows, "C", "pubsub", 5000)
    assert _has(rows, "C", "streams", 5000)


def test_kafka_rerun_is_forty_kafka_cells():
    rows = make_matrix.kafka_rerun_rows()
    assert len(rows) == 40
    assert {r["broker"] for r in rows} <= {"kafka", "kafka-tuned", "kafka-acks"}
    assert _has(rows, "B", "kafka", 3000)
    assert _has(rows, "B", "kafka", 5000)
    assert _has(rows, "C", "kafka", 2500)
    assert _has(rows, "C", "kafka-acks", 5000)
    first = {}
    for item in rows:
        first.setdefault((item["stage"], item["broker"]), item)
    assert first[("B", "kafka")]["load"] == 5000
    assert first[("C", "kafka")]["load"] == 5000


def test_full_factorial_includes_plain_kafka_and_l2500():
    rows = make_matrix.factorial_rows(1)
    brokers = {r["broker"] for r in rows if r["stage"] == "C"}
    loads = {r["load"] for r in rows if r["stage"] == "C"}
    assert "kafka" in brokers
    assert loads == {2500, 5000}
    assert all(r["broker"] != "kafka-acks" for r in rows)


def test_cells_profile_keeps_the_original_run_ids():
    import csv

    original = Path(__file__).resolve().parent / "configs" / "matrix-20261018T110200Z-c440945.csv"
    by_id = {r["run_id"]: r for r in csv.DictReader(original.open())}
    rows = make_matrix.cells_rows("rerun-20261018-kafka")
    assert len(rows) == 25
    for item in rows:
        old = by_id[item["run_id"]]
        assert item["stage"] == old["stage"] and item["broker"] == old["broker"]
        assert int(item["load"]) == int(old["load"]) and int(item["rooms"]) == int(old["rooms"])
        assert float(item["disconnects_per_min"]) == float(old["disconnects_per_min"])
        assert int(item["gateway_kill"]) == int(old["gateway_kill"]) and int(item["rep"]) == int(old["rep"])
    expected = {i for i, r in by_id.items() if r["broker"] == "kafka-tuned" and r["load"] == "5000"}
    assert {r["run_id"] for r in rows} == expected


def test_cells_profile_accepts_ids_and_presets_and_dedupes():
    spec = "009-C-streams-L5000-nu1-k0-r2, rerun-20261018-streams-c 009-C-streams-L5000-nu1-k0-r2"
    rows = make_matrix.cells_rows(spec)
    assert len(rows) == 20
    assert all(r["broker"] == "streams" and r["stage"] == "C" for r in rows)


def test_cells_profile_rejects_unknown_tokens():
    import pytest

    with pytest.raises(ValueError):
        make_matrix.cells_rows("rerun-typo")
    with pytest.raises(ValueError):
        make_matrix.cells_rows("  ")


def test_write_matrix_keeps_cell_ids(tmp_path):
    rows = make_matrix.cells_rows("103-B-kafka-tuned-L5000-nu0-k0-r0")
    out = tmp_path / "m.csv"
    make_matrix.write_matrix(rows, str(out))
    assert "103-B-kafka-tuned-L5000-nu0-k0-r0" in out.read_text()
