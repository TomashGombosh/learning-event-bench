"""Gate decisions from the 20261018T110200Z report.

kafka B L5000 delivered 34% and pinned p50 = p75 = p99 = 65.57s.
pubsub C L5000 nu=1 kept the offered load and diverged (R = 0) without being saturated.
streams B L5000 delivered everything, p50 58.1ms, p99 103.9ms.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from validity import (  # noqa: E402
    CellObservation,
    assess_cell,
    count_catchup_errors,
    empty_state,
    record_cell,
)

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "experiments" / "validate_cells.py"


def kafka_b_l5000() -> CellObservation:
    # Report section 1: 34% of deliveries, p50 plateau 65.57s, 50/50 rooms diverged.
    return CellObservation(
        run_id="053-B-kafka-L5000-nu0-k0-r0",
        stage="B",
        broker="kafka",
        load=5000,
        offered_per_sec=5000,
        attempted_per_sec=5000,
        delivery_ratio=0.34,
        offline_fraction=0,
        rooms_diverged=50,
        rooms_total=50,
        clients_connected=1000,
        clients_expected=1000,
        p50_ms=65570,
        p75_ms=65570,
        p99_ms=65570,
        hdr_p50_ms=65570,
        reservoir_p50_ms=65.6,
        sample_span_ms=100_000,
        duration_s=240,
        sample_count=50_000,
        hdr_count=3_200_000,
        catchup_errors=0,
    )


def pubsub_c() -> CellObservation:
    # Report table: pubsub C L5000 nu=1 k=0, p50 46.9, p99 132.4, achieved 0.965, 50/50 diverged, R=0.
    return CellObservation(
        run_id="050-C-pubsub-L5000-nu1-k0-r0",
        stage="C",
        broker="pubsub",
        load=5000,
        nu=1,
        kill=0,
        offered_per_sec=5000,
        attempted_per_sec=4925,
        delivery_ratio=0.995,
        offline_fraction=0.02,
        rooms_diverged=50,
        rooms_total=50,
        clients_connected=1000,
        clients_expected=1000,
        p50_ms=46.9,
        p75_ms=80,
        p99_ms=132.4,
        hdr_p50_ms=46.9,
        reservoir_p50_ms=48,
        sample_span_ms=220_000,
        duration_s=240,
        sample_count=50_000,
        hdr_count=3_000_000,
    )


def streams_b_l5000() -> CellObservation:
    # Report table: streams B L5000 p50 58.1, p99 103.9, delivery ratio 1, 0 diverged.
    return CellObservation(
        run_id="019-B-streams-L5000-nu0-k0-r0",
        stage="B",
        broker="streams",
        load=5000,
        offered_per_sec=5000,
        attempted_per_sec=5000,
        delivery_ratio=1,
        offline_fraction=0,
        rooms_diverged=0,
        rooms_total=50,
        clients_connected=1000,
        clients_expected=1000,
        p50_ms=58.1,
        p75_ms=72,
        p99_ms=103.9,
        hdr_p50_ms=58.1,
        reservoir_p50_ms=60,
        sample_span_ms=230_000,
        duration_s=240,
        sample_count=50_000,
        hdr_count=3_200_000,
    )


def test_kafka_b_l5000_fails_censored():
    verdict = assess_cell(kafka_b_l5000())
    assert verdict.status == "FAIL"
    assert "CENSORED" in verdict.failures
    assert "PLATEAU" in verdict.failures
    assert "HDR_MISMATCH" in verdict.failures


def test_pubsub_c_passes_load_and_flags_diverged():
    verdict = assess_cell(pubsub_c())
    assert verdict.status == "PASS"
    assert "DIVERGED" in verdict.flags
    assert "SATURATED" not in verdict.failures
    assert "CENSORED" not in verdict.failures


def test_streams_b_l5000_passes():
    verdict = assess_cell(streams_b_l5000())
    assert verdict.status == "PASS"
    assert verdict.failures == []
    assert "DIVERGED" not in verdict.flags
    assert "CENSORED" not in verdict.flags


def test_offline_clients_do_not_censor_a_fault_cell():
    obs = pubsub_c()
    obs.delivery_ratio = 0.80
    obs.offline_fraction = 0.12
    verdict = assess_cell(obs)
    assert verdict.status == "PASS"
    assert "CENSORED" in verdict.flags
    assert "CENSORED" not in verdict.failures
    assert "CLIENTS_OFFLINE" in verdict.flags


def test_catchup_needles_are_counted():
    text = "\n".join([
        "reconnect deadline exceeded",
        "Connect timeout while joining",
        "The operation has timed out",
        "operation has timed out",
    ])
    assert count_catchup_errors(text) == 4


def test_fail_fast_stops_on_the_first_cell_of_a_group_only():
    state = empty_state()
    assert record_cell(state, assess_cell(kafka_b_l5000()), fail_fast=True) == 42
    assert state["aborted"] is True
    later = kafka_b_l5000()
    later.run_id = "054-B-kafka-L5000-nu0-k0-r1"
    assert record_cell(state, assess_cell(later), fail_fast=True) == 1
    healthy = empty_state()
    assert record_cell(healthy, assess_cell(streams_b_l5000()), fail_fast=True) == 0
    bad_later = streams_b_l5000()
    bad_later.run_id = "020-B-streams-L5000-nu0-k0-r1"
    bad_later.delivery_ratio = 0.10
    assert record_cell(healthy, assess_cell(bad_later), fail_fast=True) == 1
    assert healthy["aborted"] is False


def _write_shard(root: Path, run_id: str, summary: dict, samples: str, log: str = "") -> None:
    shard = root / "results" / run_id / "shard-0"
    shard.mkdir(parents=True)
    (shard / "summary.json").write_text(json.dumps(summary))
    (shard / "latency_samples.csv").write_text(samples)
    (root / "results" / run_id / "gateways.log").write_text(log)


def _summary(**overrides) -> dict:
    body = {
        "config": {"WARMUP_S": 30, "DURATION_S": 240},
        "counters": {"sent": 1_350_000, "skippedWhileOffline": 0},
        "load": {"offeredPerSec": 5000, "saturated": False},
        "latency": {
            "count": 3_200_000, "p50Ms": 58.1, "p75Ms": 72, "p99Ms": 103.9,
            "reservoirMinScheduledMs": 1_000, "reservoirMaxScheduledMs": 240_000,
        },
        "delivery": {"ratio": 1},
        "rooms": 50,
        "divergedRooms": 0,
        "clientsConnected": 1000,
        "clientsExpected": 1000,
    }
    body.update(overrides)
    return body


def test_cli_kafka_fixture_fails_and_streams_passes(tmp_path: Path):
    samples = "room,total_ms,service_ms\n" + "".join(f"r,60,65.6\n" for _ in range(20))
    _write_shard(tmp_path, "053-B-kafka-L5000", _summary(
        latency={
            "count": 3_200_000, "p50Ms": 65570, "p75Ms": 65570, "p99Ms": 65570,
            "reservoirMinScheduledMs": 1_000, "reservoirMaxScheduledMs": 100_000,
        },
        delivery={"ratio": 0.34},
        divergedRooms=50,
    ), samples)
    good_samples = "room,total_ms,service_ms\n" + "".join("r,60,58.1\n" for _ in range(20))
    _write_shard(tmp_path, "019-B-streams-L5000", _summary(), good_samples)
    common = [
        sys.executable, str(SCRIPT), "--results", str(tmp_path / "results"), "--cell",
        "--load", "5000", "--rooms", "50", "--clients", "20", "--shards", "1",
    ]
    kafka = subprocess.run(
        common + ["--run-id", "053-B-kafka-L5000", "--stage", "B", "--broker", "kafka", "--fail-fast"],
        check=False, capture_output=True, text=True,
    )
    assert kafka.returncode == 42
    assert "CENSORED" in kafka.stdout
    assert "VALIDITY_ABORT" in kafka.stdout
    streams = subprocess.run(
        common + ["--run-id", "019-B-streams-L5000", "--stage", "B", "--broker", "streams"],
        check=False, capture_output=True, text=True,
    )
    assert streams.returncode == 0
    assert "PASS" in streams.stdout
    summary = subprocess.run(
        [sys.executable, str(SCRIPT), "--results", str(tmp_path / "results"), "--summary"],
        check=False, capture_output=True, text=True,
    )
    assert summary.returncode == 0
    assert "VALIDITY_SUMMARY pass=1 fail=1 total=2 aborted=1" in summary.stdout
