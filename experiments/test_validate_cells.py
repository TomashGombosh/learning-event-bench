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
        attempted_per_sec=5005,
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


def test_kafka_b_l5000_is_saturated_valid_not_a_harness_fault():
    verdict = assess_cell(kafka_b_l5000())
    assert verdict.status == "SATURATED_VALID"
    assert verdict.failures == []
    assert "PLATEAU" in verdict.flags
    state = empty_state()
    assert record_cell(state, verdict, fail_fast=True) == 0
    assert state["aborted"] is False


def test_pubsub_c_passes_load_and_flags_diverged():
    verdict = assess_cell(pubsub_c())
    assert verdict.status == "PASS"
    assert "DIVERGED" in verdict.flags
    assert verdict.failures == []


def test_streams_b_l5000_passes():
    verdict = assess_cell(streams_b_l5000())
    assert verdict.status == "PASS"
    assert verdict.failures == []
    assert "DIVERGED" not in verdict.flags
    assert "CENSORED" not in verdict.flags


def test_offline_clients_do_not_censor_a_fault_cell():
    obs = pubsub_c()
    # 0.87 sits inside 0.02 of (1 - 0.12) = 0.88, so the shortfall is the offline clients.
    obs.delivery_ratio = 0.87
    obs.offline_fraction = 0.12
    verdict = assess_cell(obs)
    assert verdict.status == "PASS"
    assert "CENSORED" not in verdict.flags
    assert "CENSORED" not in verdict.failures
    assert "CLIENTS_OFFLINE" in verdict.flags


def test_delivery_far_below_online_clients_is_saturated_valid():
    """20261019T085759Z-7e40e22 cell 003: delivery 0.018, offline 0.314, attempted 0.999.

    p50 66s and p99 200s are not a plateau. The same gap on pubsub is saturation too.
    """
    def cell(broker: str) -> CellObservation:
        return CellObservation(
            run_id=f"003-C-{broker}-L5000-nu1-k1-r0",
            stage="C", broker=broker, load=5000, nu=1, kill=1,
            offered_per_sec=5000, attempted_per_sec=4995,
            delivery_ratio=0.018, offline_fraction=0.314,
            rooms_diverged=50, rooms_total=50,
            clients_connected=1000, clients_expected=1000,
            p50_ms=66_000, p75_ms=120_000, p99_ms=200_000,
            hdr_p50_ms=66_000, reservoir_p50_ms=66_000,
            sample_span_ms=50_000, duration_s=60,
            sample_count=1_000, hdr_count=50_000,
        )
    for broker in ("kafka", "pubsub"):
        verdict = assess_cell(cell(broker))
        assert verdict.status == "SATURATED_VALID", broker
        assert verdict.failures == [], broker
        assert "CLIENTS_OFFLINE" in verdict.flags, broker
        assert "DIVERGED" in verdict.flags, broker
        assert "CENSORED" not in verdict.flags, broker
        assert "PLATEAU" not in verdict.flags, broker
        assert record_cell(empty_state(), verdict, fail_fast=True) == 0


def test_offline_explained_loss_is_clients_offline_for_every_broker():
    """kafka-tuned and pubsub at delivery 0.960, offline 0.033 are the same class.

    Expected delivery is 0.967. 0.960 is inside the 0.02 tolerance, so this is
    CLIENTS_OFFLINE, not SATURATED_VALID.
    """
    def cell(broker: str) -> CellObservation:
        return CellObservation(
            run_id=f"020-C-{broker}-L2500-nu1-k0-r0",
            stage="C", broker=broker, load=2500, nu=1,
            offered_per_sec=2500, attempted_per_sec=2490,
            delivery_ratio=0.960, offline_fraction=0.033,
            rooms_diverged=0, rooms_total=25,
            clients_connected=500, clients_expected=500,
            p50_ms=40, p75_ms=80, p99_ms=150,
            hdr_p50_ms=40, reservoir_p50_ms=42,
            sample_span_ms=55_000, duration_s=60,
            sample_count=20_000, hdr_count=1_000_000,
        )
    for broker in ("kafka-tuned", "pubsub"):
        verdict = assess_cell(cell(broker))
        assert verdict.status == "PASS", broker
        assert verdict.failures == [], broker
        assert "CLIENTS_OFFLINE" in verdict.flags, broker
        assert "CENSORED" not in verdict.flags, broker


def test_delivery_on_the_offline_floor_is_not_saturated():
    obs = pubsub_c()
    obs.offline_fraction = 0.033
    obs.delivery_ratio = (1 - 0.033) - 0.02
    verdict = assess_cell(obs)
    assert verdict.status == "PASS"
    assert "CLIENTS_OFFLINE" in verdict.flags


def test_catchup_needles_are_counted():
    text = "\n".join([
        "reconnect deadline exceeded",
        "Connect timeout while joining",
        "The operation has timed out",
        "operation has timed out",
    ])
    assert count_catchup_errors(text) == 4


def harness_short_rate() -> CellObservation:
    cell = streams_b_l5000()
    cell.run_id = "harness-short"
    cell.attempted_per_sec = 4000
    return cell


def test_fail_fast_stops_only_on_a_harness_fault():
    state = empty_state()
    assert record_cell(state, assess_cell(kafka_b_l5000()), fail_fast=True) == 0
    assert state["aborted"] is False
    # Same broker x scenario later, even a harness fault, does not abort: the canary already ran.
    later = harness_short_rate()
    later.broker = "kafka"
    later.stage = "B"
    later.run_id = "054-B-kafka-short"
    assert record_cell(state, assess_cell(later), fail_fast=True) == 1
    assert state["aborted"] is False
    fresh = empty_state()
    assert record_cell(fresh, assess_cell(harness_short_rate()), fail_fast=True) == 42
    assert fresh["aborted"] is True
    assert "ATTEMPTED" in fresh["reason"]


def test_smoke_20261019_cells():
    """Numbers from run 20261019T065355Z-d2057c2 (17/17, pass=15 fail=3 before this gate)."""
    kafka_b = CellObservation(
        run_id="005-B-kafka-L5000-nu0-k0-r0", stage="B", broker="kafka", load=5000,
        offered_per_sec=5000, attempted_per_sec=5005.4, delivery_ratio=0.412, offline_fraction=0,
        rooms_diverged=50, rooms_total=50, clients_connected=1000, clients_expected=1000,
        p50_ms=27295.7, p75_ms=32000, p99_ms=40992.8, hdr_p50_ms=27295.7, reservoir_p50_ms=27000,
        sample_span_ms=20_000, duration_s=60, sample_count=50_000, hdr_count=1_300_000, catchup_errors=0,
    )
    tuned = CellObservation(
        run_id="006-B-kafka-tuned-L5000-nu0-k0-r0", stage="B", broker="kafka-tuned", load=5000,
        offered_per_sec=5000, attempted_per_sec=5005.4, delivery_ratio=1, offline_fraction=0,
        rooms_diverged=0, clients_connected=1000, clients_expected=1000,
        p50_ms=181.5, p75_ms=300, p99_ms=653.8, hdr_p50_ms=181.5, reservoir_p50_ms=180,
        sample_span_ms=55_000, duration_s=60, sample_count=50_000, hdr_count=3_200_000,
    )
    acks = CellObservation(
        run_id="007-B-kafka-acks-L5000-nu0-k0-r0", stage="B", broker="kafka-acks", load=5000,
        offered_per_sec=5000, attempted_per_sec=5005.5, delivery_ratio=1, offline_fraction=0,
        rooms_diverged=0, clients_connected=1000, clients_expected=1000,
        p50_ms=164.0, p75_ms=280, p99_ms=611.3, hdr_p50_ms=164.0, reservoir_p50_ms=170,
        sample_span_ms=55_000, duration_s=60, sample_count=50_000, hdr_count=3_200_000,
    )
    tuned_fault = CellObservation(
        run_id="011-C-kafka-tuned-L2500-nu1-k0-r0", stage="C", broker="kafka-tuned", load=2500, nu=1,
        offered_per_sec=2500, attempted_per_sec=2501.6, delivery_ratio=0.704, offline_fraction=0.271,
        rooms_diverged=24, rooms_total=25, clients_connected=500, clients_expected=500,
        p50_ms=4927.5, p75_ms=8000, p99_ms=22478.8, hdr_p50_ms=4927.5, reservoir_p50_ms=5000,
        sample_span_ms=50_000, duration_s=60, sample_count=20_000, hdr_count=800_000,
    )
    plain_c = CellObservation(
        run_id="012-C-kafka-L2500-nu0.01-k0-r0", stage="C", broker="kafka", load=2500, nu=0.01,
        offered_per_sec=2500, attempted_per_sec=2501.6, delivery_ratio=0.999, offline_fraction=0,
        rooms_diverged=0, clients_connected=500, clients_expected=500,
        p50_ms=1877.0, p75_ms=2500, p99_ms=3528.7, hdr_p50_ms=1877.0, reservoir_p50_ms=1900,
        sample_span_ms=55_000, duration_s=60, sample_count=40_000, hdr_count=1_400_000,
    )
    kafka_c = CellObservation(
        run_id="013-C-kafka-L5000-nu0.01-k0-r0", stage="C", broker="kafka", load=5000, nu=0.01,
        offered_per_sec=5000, attempted_per_sec=5005.5, delivery_ratio=0.393, offline_fraction=0,
        rooms_diverged=50, clients_connected=1000, clients_expected=1000,
        p50_ms=26558.5, p75_ms=31000, p99_ms=43057.2, hdr_p50_ms=26558.5, reservoir_p50_ms=26000,
        sample_span_ms=15_000, duration_s=60, sample_count=50_000, hdr_count=1_200_000,
    )
    pubsub = CellObservation(
        run_id="015-C-pubsub-L5000-nu1-k0-r0", stage="C", broker="pubsub", load=5000, nu=1,
        offered_per_sec=5000, attempted_per_sec=5005.4, delivery_ratio=0.961, offline_fraction=0.034,
        rooms_diverged=50, clients_connected=1000, clients_expected=1000,
        p50_ms=53.0, p75_ms=80, p99_ms=149.2, hdr_p50_ms=53.0, reservoir_p50_ms=55,
        sample_span_ms=55_000, duration_s=60, sample_count=50_000, hdr_count=2_900_000,
    )
    assert assess_cell(kafka_b).status == "SATURATED_VALID"
    assert assess_cell(kafka_c).status == "SATURATED_VALID"
    assert assess_cell(tuned).status == "PASS"
    assert assess_cell(acks).status == "PASS"
    assert assess_cell(plain_c).status == "PASS"
    fault = assess_cell(tuned_fault)
    # 0.704 is below (1 - 0.271) - 0.02 = 0.709, so the gap is the broker plus the offline clients.
    assert fault.status == "SATURATED_VALID"
    assert "CLIENTS_OFFLINE" in fault.flags
    assert "DIVERGED" in fault.flags
    assert "CENSORED" not in fault.flags
    diverged = assess_cell(pubsub)
    assert diverged.status == "PASS"
    assert "DIVERGED" in diverged.flags
    assert diverged.failures == []
    # A complete delivery whose reservoir covers only the start of the window is the recorder bug.
    recorder = streams_b_l5000()
    recorder.duration_s = 60
    recorder.sample_span_ms = 10_000
    recorder.delivery_ratio = 1
    assert "SAMPLES_WINDOW" in assess_cell(recorder).failures


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
    assert kafka.returncode == 0, kafka.stdout + kafka.stderr
    assert "SATURATED_VALID" in kafka.stdout
    assert "VALIDITY_ABORT" not in kafka.stdout
    streams = subprocess.run(
        common + ["--run-id", "019-B-streams-L5000", "--stage", "B", "--broker", "streams"],
        check=False, capture_output=True, text=True,
    )
    assert streams.returncode == 0
    assert "PASS" in streams.stdout
    missing = subprocess.run(
        common + ["--run-id", "missing-cell", "--stage", "A", "--broker", "streams", "--fail-fast"],
        check=False, capture_output=True, text=True,
    )
    assert missing.returncode == 42
    assert "MISSING" in missing.stdout
    summary = subprocess.run(
        [sys.executable, str(SCRIPT), "--results", str(tmp_path / "results"), "--summary"],
        check=False, capture_output=True, text=True,
    )
    assert summary.returncode == 0
    assert "VALIDITY_SUMMARY pass=1 saturated_valid=1 fail=1 total=3 aborted=1" in summary.stdout
