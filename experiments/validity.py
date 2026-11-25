"""Per-cell validity gate.

FAIL is a harness fault, and it is the only status that fail-fast aborts on:
- Missing or incomplete shard summaries (shard crash, timeout, or missing files),
- Fewer clients connected at start than the cell asked for,
- Catch-up errors in gateways.log exceeding the threshold,
- A latency reservoir that stops early despite complete delivery (recorder bug),
- Generator median schedule lag exceeding SCHEDULE_LAG_P50_MAX_MS (250 ms), indicating
  loadgen CPU starvation / timer drift on the generator's own event loop, or
- Generator attempted rate falling below HARNESS_ATTEMPTED_FLOOR (0.99) when the SUT
  is not saturated, or below SATURATED_ATTEMPTED_FLOOR (0.50) under SUT saturation.

A cell whose loadgen health signals are intact (all shards present and complete,
all clients connected, median generator schedule lag <= 250 ms, attempted >= 0.99
or >= 0.50 under genuine SUT saturation, catch-up errors within bound) while
delivery fell more than OFFLINE_DELIVERY_TOLERANCE below (1 - offline) is
SATURATED_VALID: genuine SUT saturation, recorded as a valid measured outcome.
It is kept for throughput/CSA analysis, excluded from non-saturated latency
summaries, and does not abort the run. A shortfall inside that tolerance is
CLIENTS_OFFLINE on every broker. Pubsub divergence is expected (no replay) and
is a flag only.

profile=smoke sets VALIDITY_FAIL_FAST=0, so a smoke run records every cell and
writes aborted=0 even when a cell FAILs. csa-smoke, csa-full, kafka-rerun and
full default it to 1.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path

CATCHUP_NEEDLES = (
    "reconnect deadline exceeded",
    "connect timeout",
    "operation has timed out",
)
DELIVERY_FLOOR = 0.99
# Harness fault: the generator itself did not attempt the offered load.
HARNESS_ATTEMPTED_FLOOR = 0.99
# Under genuine SUT saturation (backpressure/reconnects), allow attempted >= 50%
# as long as median generator schedule lag stays healthy (<= SCHEDULE_LAG_P50_MAX_MS).
SATURATED_ATTEMPTED_FLOOR = 0.50
# Matches apps/load-generator/load-report.ts generator schedule lag threshold.
SCHEDULE_LAG_P50_MAX_MS = 250.0
OFFLINE_FLAG = 0.05
# Delivery this far below (1 - offline) is the broker, not clients that were offline.
OFFLINE_DELIVERY_TOLERANCE = 0.02
# p50, p75 and p99 sitting on the same large bin (the 65.57s Kafka plateau).
PLATEAU_MS = 1000.0
PLATEAU_REL = 0.05
# Reservoir p50 vs HDR p50. The censored Kafka cell was ~1000x apart; 2x is already not the same distribution.
HDR_REL = 0.50
WINDOW_COVER = 0.50


@dataclass
class CellObservation:
    run_id: str
    stage: str
    broker: str
    load: float = 0
    nu: float = 0
    kill: int = 0
    offered_per_sec: float = 0
    attempted_per_sec: float = 0
    delivery_ratio: float = 1
    offline_fraction: float = 0
    rooms_diverged: int = 0
    rooms_total: int = 0
    clients_connected: int = 0
    clients_expected: int = 0
    p50_ms: float = 0
    p75_ms: float = 0
    p99_ms: float = 0
    hdr_p50_ms: float = 0
    reservoir_p50_ms: float = 0
    sample_span_ms: float = 0
    duration_s: float = 0
    sample_count: int = 0
    hdr_count: int = 0
    catchup_errors: int = 0
    shard_missing: bool = False
    incomplete: bool = False
    schedule_lag_p50_ms: float = 0.0


@dataclass
class CellVerdict:
    run_id: str
    stage: str
    broker: str
    status: str
    failures: list[str] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    delivery_ratio: float = 1
    attempted_ratio: float = 1
    offline_fraction: float = 0
    catchup_errors: int = 0
    clients_connected: int = 0
    clients_expected: int = 0
    schedule_lag_p50_ms: float = 0.0

    def line(self) -> str:
        marks = self.failures + [flag for flag in self.flags if flag not in self.failures]
        suffix = (" " + " ".join(marks)) if marks else ""
        return (
            f"VALIDITY {self.run_id} {self.status} "
            f"delivery={self.delivery_ratio:.3f} attempted={self.attempted_ratio:.3f} "
            f"offline={self.offline_fraction:.3f} "
            f"connected={self.clients_connected}/{self.clients_expected} "
            f"catchup_errors={self.catchup_errors} "
            f"lag_p50={self.schedule_lag_p50_ms:.1f}ms{suffix}"
        )


def _finite(value: float) -> float:
    if value is None or not math.isfinite(value):
        return 0.0
    return float(value)


def count_catchup_errors(text: str) -> int:
    folded = text.lower()
    return sum(folded.count(needle) for needle in CATCHUP_NEEDLES)


def expected_delivery(offline_fraction: float) -> float:
    """Share of events online clients should still receive."""
    return max(0.0, 1.0 - max(0.0, _finite(offline_fraction)))


def broker_saturated(
    delivery_ratio: float,
    offline_fraction: float,
    attempted_ratio: float,
    *,
    shard_missing: bool = False,
    incomplete: bool = False,
    schedule_lag_p50_ms: float = 0.0,
) -> bool:
    """Loadgen health signals passed, and delivery fell below what online clients should receive.

    Expected delivery is (1 - offline). A gap larger than OFFLINE_DELIVERY_TOLERANCE is
    SUT saturation. When the SUT is saturated, attempted_ratio down to SATURATED_ATTEMPTED_FLOOR
    (0.50) is still accepted as SATURATED_VALID provided all shards completed and median
    generator schedule lag stayed <= SCHEDULE_LAG_P50_MAX_MS (250 ms).
    """
    if shard_missing or incomplete or _finite(schedule_lag_p50_ms) > SCHEDULE_LAG_P50_MAX_MS:
        return False
    floor = expected_delivery(offline_fraction) - OFFLINE_DELIVERY_TOLERANCE
    if delivery_ratio >= floor:
        return False
    return attempted_ratio >= SATURATED_ATTEMPTED_FLOOR


def loss_explained_by_offline(delivery_ratio: float, offline_fraction: float) -> bool:
    """Delivery is under 99%, but within tolerance of (1 - offline), and some clients were offline."""
    if offline_fraction <= 0 or delivery_ratio >= DELIVERY_FLOOR:
        return False
    floor = expected_delivery(offline_fraction) - OFFLINE_DELIVERY_TOLERANCE
    return delivery_ratio >= floor


def _saturated_valid(obs: CellObservation, attempted_ratio: float) -> bool:
    return broker_saturated(
        obs.delivery_ratio,
        obs.offline_fraction,
        attempted_ratio,
        shard_missing=obs.shard_missing,
        incomplete=obs.incomplete,
        schedule_lag_p50_ms=obs.schedule_lag_p50_ms,
    )


def assess_cell(obs: CellObservation, *, catchup_error_max: int = 10) -> CellVerdict:
    failures: list[str] = []
    flags: list[str] = []
    offered = obs.offered_per_sec
    attempted_ratio = (obs.attempted_per_sec / offered) if offered > 0 else 1.0
    saturated_valid = _saturated_valid(obs, attempted_ratio)

    if obs.shard_missing:
        failures.append("MISSING")
    if obs.incomplete:
        failures.append("INCOMPLETE")
    if _finite(obs.schedule_lag_p50_ms) > SCHEDULE_LAG_P50_MAX_MS:
        failures.append("GENERATOR_LAG")
    if offered > 0 and attempted_ratio < HARNESS_ATTEMPTED_FLOOR:
        if not saturated_valid:
            failures.append("ATTEMPTED")
        else:
            flags.append("ATTEMPTED_BELOW_FLOOR")
    if obs.catchup_errors > catchup_error_max:
        failures.append("CATCHUP_ERRORS")
    if obs.clients_expected > 0 and obs.clients_connected < obs.clients_expected:
        failures.append("CLIENTS_CONNECTED")

    window_ms = max(0.0, obs.duration_s) * 1000
    covers = obs.hdr_count > 0 and obs.sample_count > 0 and (window_ms <= 0 or obs.sample_span_ms >= WINDOW_COVER * window_ms)
    # A short reservoir span while the broker delivered is the recorder stopping early.
    # A short span together with a delivery shortfall is the undelivered tail, not that bug.
    if not covers and obs.delivery_ratio >= DELIVERY_FLOOR:
        failures.append("SAMPLES_WINDOW")

    explained = loss_explained_by_offline(obs.delivery_ratio, obs.offline_fraction)
    # CENSORED is an unexplained shortfall. Saturation and offline clients already name the cause.
    if obs.delivery_ratio < DELIVERY_FLOOR and not saturated_valid and not explained:
        flags.append("CENSORED")
    if obs.offline_fraction > OFFLINE_FLAG or explained:
        flags.append("CLIENTS_OFFLINE")
    if obs.rooms_diverged > 0:
        flags.append("DIVERGED")
    if obs.p50_ms >= PLATEAU_MS and obs.p75_ms > 0 and obs.p99_ms > 0:
        if abs(obs.p75_ms - obs.p50_ms) / obs.p50_ms < PLATEAU_REL and abs(obs.p99_ms - obs.p50_ms) / obs.p50_ms < PLATEAU_REL:
            flags.append("PLATEAU")
    hdr = obs.hdr_p50_ms
    reservoir = obs.reservoir_p50_ms
    if hdr > 0 and reservoir > 0 and abs(hdr - reservoir) / max(hdr, reservoir) > HDR_REL:
        flags.append("HDR_MISMATCH")

    def unique(items: list[str]) -> list[str]:
        seen: list[str] = []
        for item in items:
            if item not in seen:
                seen.append(item)
        return seen

    failures = unique(failures)
    flags = unique(flags)
    if failures:
        status = "FAIL"
    elif saturated_valid:
        status = "SATURATED_VALID"
    else:
        status = "PASS"
    return CellVerdict(
        run_id=obs.run_id,
        stage=obs.stage,
        broker=obs.broker,
        status=status,
        failures=failures,
        flags=flags,
        delivery_ratio=obs.delivery_ratio,
        attempted_ratio=attempted_ratio,
        offline_fraction=obs.offline_fraction,
        catchup_errors=obs.catchup_errors,
        clients_connected=obs.clients_connected,
        clients_expected=obs.clients_expected,
        schedule_lag_p50_ms=_finite(obs.schedule_lag_p50_ms),
    )


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * q / 100
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    return ordered[low] * (high - rank) + ordered[high] * (rank - low)


def _window_s(summary: dict) -> float:
    config = summary.get("config") or {}
    return max(0.001, float(config.get("WARMUP_S", 0)) + float(config.get("DURATION_S", 0)))


def _duration_s(summary: dict) -> float:
    config = summary.get("config") or {}
    return float(config.get("DURATION_S", 0))


def load_observation(
    results: Path,
    *,
    run_id: str,
    stage: str,
    broker: str,
    load: float,
    nu: float,
    kill: int,
    rooms: int,
    clients: int,
    shards: int,
) -> CellObservation:
    run_dir = results / run_id
    paths = sorted(run_dir.glob("shard-*/summary.json"))
    expected_shards = min(rooms, shards) if rooms > 0 else shards
    summaries = [json.loads(path.read_text()) for path in paths]
    shard_missing = not summaries or len(summaries) < expected_shards

    attempted = 0.0
    offered = 0.0
    sent = skipped = 0.0
    ratios: list[float] = []
    rooms_diverged = 0
    rooms_total = 0
    connected = 0
    expected_clients = 0
    p50 = p75 = p99 = 0.0
    schedule_lag_p50 = 0.0
    incomplete = False
    hdr_count = 0
    span_min = math.inf
    span_max = -math.inf
    sample_count = 0
    reservoir_values: list[float] = []

    for summary, path in zip(summaries, paths):
        counters = summary.get("counters") or {}
        window = _window_s(summary)
        sent += float(counters.get("sent", 0))
        skipped += float(counters.get("skippedWhileOffline", 0))
        attempted += (float(counters.get("sent", 0)) + float(counters.get("skippedWhileOffline", 0))) / window
        load_block = summary.get("load") or {}
        offered += float(load_block.get("offeredPerSec") or 0)
        delivery = summary.get("delivery") or {}
        if "ratio" in delivery:
            ratios.append(float(delivery["ratio"]))
        rooms_diverged += int(summary.get("divergedRooms") or 0)
        rooms_total += int(summary.get("rooms") or 0)
        connected += int(summary.get("clientsConnected") or 0)
        expected_clients += int(summary.get("clientsExpected") or 0)
        if bool(summary.get("incomplete")):
            incomplete = True
        latency = summary.get("latency") or {}
        shard_lag_p50 = float(latency.get("scheduleLagP50Ms") or load_block.get("scheduleLagP50Ms") or 0)
        schedule_lag_p50 = max(schedule_lag_p50, shard_lag_p50)
        shard_p50 = float(latency.get("p50Ms") or 0)
        if shard_p50 >= p50:
            p50 = shard_p50
            p75 = float(latency.get("p75Ms") or 0)
            p99 = float(latency.get("p99Ms") or 0)
        hdr_count += int(latency.get("count") or 0)
        low = float(latency.get("reservoirMinScheduledMs") or 0)
        high = float(latency.get("reservoirMaxScheduledMs") or 0)
        if high > low:
            span_min = min(span_min, low)
            span_max = max(span_max, high)
        samples = path.parent / "latency_samples.csv"
        if samples.is_file():
            reservoir_values.extend(_service_column(samples))

    if offered <= 0 and summaries:
        offered = float(load)
    offline = (skipped / (sent + skipped)) if (sent + skipped) else 0.0
    delivery_ratio = min(ratios) if ratios else (0.0 if summaries else 0.0)
    duration = max((_duration_s(summary) for summary in summaries), default=0.0)
    sample_count = len(reservoir_values)
    span = 0.0 if not math.isfinite(span_min) else max(0.0, span_max - span_min)
    log_path = run_dir / "gateways.log"
    catchup = count_catchup_errors(log_path.read_text(errors="replace")) if log_path.is_file() else 0
    if expected_clients <= 0:
        expected_clients = rooms * clients

    return CellObservation(
        run_id=run_id,
        stage=stage,
        broker=broker,
        load=load,
        nu=nu,
        kill=kill,
        offered_per_sec=_finite(offered if offered > 0 else load),
        attempted_per_sec=_finite(attempted),
        delivery_ratio=_finite(delivery_ratio),
        offline_fraction=_finite(offline),
        rooms_diverged=rooms_diverged,
        rooms_total=rooms_total,
        clients_connected=connected,
        clients_expected=expected_clients,
        p50_ms=_finite(p50),
        p75_ms=_finite(p75),
        p99_ms=_finite(p99),
        hdr_p50_ms=_finite(p50),
        reservoir_p50_ms=_finite(_percentile(reservoir_values, 50)),
        sample_span_ms=_finite(span),
        duration_s=_finite(duration),
        sample_count=sample_count,
        hdr_count=hdr_count,
        catchup_errors=catchup,
        shard_missing=shard_missing,
        incomplete=incomplete,
        schedule_lag_p50_ms=_finite(schedule_lag_p50),
    )


def _service_column(path: Path) -> list[float]:
    lines = path.read_text().splitlines()
    if len(lines) < 2:
        return []
    header = lines[0].split(",")
    try:
        index = header.index("service_ms")
    except ValueError:
        return []
    values = []
    for line in lines[1:]:
        if not line:
            continue
        parts = line.split(",")
        if len(parts) <= index or parts[index] == "":
            continue
        values.append(float(parts[index]))
    return values


def empty_state() -> dict:
    return {"cells": [], "groups_seen": [], "aborted": False, "reason": ""}


def load_state(path: Path) -> dict:
    if not path.is_file():
        return empty_state()
    data = json.loads(path.read_text())
    data.setdefault("cells", [])
    data.setdefault("groups_seen", [])
    data.setdefault("aborted", False)
    data.setdefault("reason", "")
    return data


def group_key(stage: str, broker: str, scheme: str = "") -> str:
    """Fail-fast group. A non-empty scheme keeps Yjs and ShareDB canaries apart."""
    if scheme:
        return f"{stage}|{scheme}|{broker}"
    return f"{stage}|{broker}"


def record_cell(state: dict, verdict: CellVerdict, *, fail_fast: bool, scheme: str = "") -> int:
    """Remember the cell. Returns 42 when this is the first cell of its group and it failed."""
    group = group_key(verdict.stage, verdict.broker, scheme)
    first = group not in state["groups_seen"]
    if first:
        state["groups_seen"].append(group)
    state["cells"] = [cell for cell in state["cells"] if cell.get("run_id") != verdict.run_id]
    state["cells"].append(asdict(verdict))
    if fail_fast and first and verdict.status == "FAIL":
        state["aborted"] = True
        state["reason"] = f"group {group} cell {verdict.run_id}: {' '.join(verdict.failures)}"
        return 42
    # SATURATED_VALID is a measured outcome. Only FAIL aborts, and only on the first cell.
    return 0 if verdict.status != "FAIL" else 1


def summary_counts(state: dict) -> tuple[int, int, int, int]:
    cells = state.get("cells") or []
    passed = sum(1 for cell in cells if cell.get("status") == "PASS")
    saturated = sum(1 for cell in cells if cell.get("status") == "SATURATED_VALID")
    failed = sum(1 for cell in cells if cell.get("status") == "FAIL")
    return passed, saturated, failed, len(cells)


def summary_line(state: dict) -> str:
    passed, saturated, failed, total = summary_counts(state)
    aborted = 1 if state.get("aborted") else 0
    return (
        f"VALIDITY_SUMMARY pass={passed} saturated_valid={saturated} "
        f"fail={failed} total={total} aborted={aborted}"
    )


def table(state: dict) -> str:
    rows = ["VALIDITY_TABLE", f"{'run_id':<52} {'status':<15} {'delivery':>8} {'attempted':>9}  marks"]
    for cell in state.get("cells") or []:
        marks = " ".join(cell.get("failures") or [])
        extra = [flag for flag in (cell.get("flags") or []) if flag not in (cell.get("failures") or [])]
        if extra:
            marks = (marks + " " + " ".join(extra)).strip()
        rows.append(
            f"{cell.get('run_id', ''):<52} {cell.get('status', ''):<15} "
            f"{float(cell.get('delivery_ratio') or 0):8.3f} {float(cell.get('attempted_ratio') or 0):9.3f}  {marks}"
        )
    rows.append(summary_line(state))
    return "\n".join(rows)
