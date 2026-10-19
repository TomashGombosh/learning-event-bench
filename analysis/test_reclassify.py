"""Relabel collected runs with the delivery-vs-(1 - offline) gate."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import reclassify_validity as reclassify  # noqa: E402


def _summary(*, offered: float, sent: float, skipped: float, delivery: float, diverged: int) -> dict:
    return {
        "config": {"WARMUP_S": 5, "DURATION_S": 60},
        "counters": {"sent": sent, "skippedWhileOffline": skipped},
        "load": {"offeredPerSec": offered, "saturated": False},
        "latency": {
            "count": 50_000,
            "p50Ms": 66_000,
            "p75Ms": 120_000,
            "p99Ms": 200_000,
            "reservoirMinScheduledMs": 1_000,
            "reservoirMaxScheduledMs": 55_000,
        },
        "delivery": {"ratio": delivery},
        "rooms": 50,
        "divergedRooms": diverged,
        "clientsConnected": 1000,
        "clientsExpected": 1000,
    }


def _write(results: Path, run_id: str, summary: dict) -> None:
    shard = results / run_id / "shard-0"
    shard.mkdir(parents=True)
    (shard / "summary.json").write_text(json.dumps(summary))


def test_reclassify_smoke_saturation_and_offline_explained_loss(tmp_path: Path, capsys):
    """Fixtures from 20261019T085759Z (cell 003) and the kafka-tuned / pubsub 0.960 pair."""
    window = 65.0
    # (a) delivery 0.018, offline 0.314, attempted 0.999 -> SATURATED_VALID
    offered_a = 5000.0
    total_a = 0.999 * offered_a * window
    _write(tmp_path, "003-C-kafka-L5000-nu1-k1-r0", _summary(
        offered=offered_a, sent=total_a * (1 - 0.314), skipped=total_a * 0.314,
        delivery=0.018, diverged=50,
    ))
    # (b) delivery 0.960, offline 0.033 -> PASS + CLIENTS_OFFLINE for both brokers
    offered_b = 2500.0
    total_b = offered_b * window
    for run_id in (
        "021-C-kafka-tuned-L2500-nu1-k0-r0",
        "022-C-pubsub-L2500-nu1-k0-r0",
    ):
        _write(tmp_path, run_id, _summary(
            offered=offered_b, sent=total_b * (1 - 0.033), skipped=total_b * 0.033,
            delivery=0.960, diverged=0,
        ))
    original = {
        "cells": [
            {"run_id": "003-C-kafka-L5000-nu1-k1-r0", "status": "PASS"},
            {"run_id": "021-C-kafka-tuned-L2500-nu1-k0-r0", "status": "SATURATED_VALID"},
            {"run_id": "022-C-pubsub-L2500-nu1-k0-r0", "status": "PASS"},
        ],
        "groups_seen": [],
        "aborted": False,
        "reason": "",
    }
    (tmp_path / "validity.json").write_text(json.dumps(original))

    state = reclassify.reclassify(tmp_path)
    out = capsys.readouterr().out
    by_id = {cell["run_id"]: cell for cell in state["cells"]}

    kafka = by_id["003-C-kafka-L5000-nu1-k1-r0"]
    assert kafka["status"] == "SATURATED_VALID"
    assert "CLIENTS_OFFLINE" in kafka["flags"]
    assert "DIVERGED" in kafka["flags"]
    assert abs(kafka["attempted_ratio"] - 0.999) < 1e-9
    assert abs(kafka["offline_fraction"] - 0.314) < 1e-9
    assert abs(kafka["delivery_ratio"] - 0.018) < 1e-9

    for run_id in ("021-C-kafka-tuned-L2500-nu1-k0-r0", "022-C-pubsub-L2500-nu1-k0-r0"):
        cell = by_id[run_id]
        assert cell["status"] == "PASS", run_id
        assert "CLIENTS_OFFLINE" in cell["flags"], run_id
        assert "CENSORED" not in cell["flags"], run_id
        assert abs(cell["delivery_ratio"] - 0.960) < 1e-9
        assert abs(cell["offline_fraction"] - 0.033) < 1e-9

    assert "003-C-kafka-L5000-nu1-k1-r0 PASS -> SATURATED_VALID" in out
    assert "021-C-kafka-tuned-L2500-nu1-k0-r0 SATURATED_VALID -> PASS" in out
    assert "022-C-pubsub-L2500-nu1-k0-r0 PASS ->" not in out
    written = json.loads((tmp_path / "validity-reclassified.json").read_text())
    assert written["cells"][0]["status"] == "SATURATED_VALID"
    assert json.loads((tmp_path / "validity.json").read_text()) == original


def test_reclassify_prefers_matrix_over_the_run_id(tmp_path: Path):
    window = 65.0
    total = 5000.0 * window
    _write(tmp_path, "custom-id", _summary(
        offered=5000, sent=total, skipped=0, delivery=0.40, diverged=0,
    ))
    matrix = tmp_path / "matrix.csv"
    matrix.write_text(
        "stage,broker,load,rooms,clients,rate,disconnects_per_min,gateway_kill,rep,run_id\n"
        "B,kafka,5000,50,20,100,0,0,0,custom-id\n"
    )
    state = reclassify.reclassify(tmp_path, matrix)
    cell = state["cells"][0]
    assert cell["status"] == "SATURATED_VALID"
    assert cell["broker"] == "kafka"
    assert cell["stage"] == "B"
