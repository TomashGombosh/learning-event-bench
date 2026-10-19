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
