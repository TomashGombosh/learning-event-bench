#!/usr/bin/env python3
"""Generates the randomized run plan (experiments/configs/matrix.csv).

Stage A (calibration): low load, every durable broker, 2 reps.
Stage B (latency, H1/H4): broker x load{1000,2500,5000} x REPS, n=20, no faults.
Stage C (reliability, H2/H3/H5): broker x load{2500,5000} x disconnects{0.01,1 per min}
         x gateway kill{no,yes} x REPS. Plain kafka is included. L2500 is the
         comparable load; L5000 stays as a stress test.

Modes (first argument):
  smoke         one short cell per broker x scenario, including the configs that
                invalidated the 20261018 run. About 45-60 min at a 60s window.
  kafka-rerun   about 40 Kafka-only cells (plain, tuned, idempotent off).
  cells SPEC    only the listed cells. SPEC is a comma or space separated list of
                run ids from an earlier matrix (for example
                008-C-kafka-tuned-L5000-nu0.01-k0-r3) and/or preset names from
                PRESETS. Each cell keeps its original run id, so its results can
                replace the old cell of that run (see experiments/fetch_results.sh).
  <reps>        full factorial. reps=5 is the long run.

Add --dry-run after the arguments to validate and print the plan without writing it.

The first cell of each broker x scenario is the risky one (highest load, faults on).
Full and kafka-rerun runs abort if that cell fails the validity gate.
"""
from __future__ import annotations

import csv
import random
import re
import sys

SEED = 20261101
CLIENTS, RATE = 20, 5.0


def rooms_for(load: float) -> int:
    return int(load / (CLIENTS * RATE))


def row(stage, broker, load, nu, kill, rep):
    return dict(
        stage=stage, broker=broker, load=load, rooms=rooms_for(load), clients=CLIENTS, rate=RATE,
        disconnects_per_min=nu, gateway_kill=kill, rep=rep,
    )


def order_with_canaries(rows: list[dict]) -> list[dict]:
    """One risky cell per broker x scenario first, then the rest in a fixed shuffle."""
    grouped: dict[tuple, list] = {}
    for item in rows:
        grouped.setdefault((item["stage"], item["broker"]), []).append(item)
    canaries, rest = [], []
    for items in grouped.values():
        items.sort(key=lambda r: (r["load"], r["disconnects_per_min"], r["gateway_kill"], -r["rep"]), reverse=True)
        canaries.append(items[0])
        rest.extend(items[1:])
    canaries.sort(key=lambda r: (r["stage"], r["broker"]))
    a_rest = [r for r in rest if r["stage"] == "A"]
    tail = [r for r in rest if r["stage"] != "A"]
    random.Random(SEED).shuffle(a_rest)
    random.Random(SEED + 1).shuffle(tail)
    a_can = [r for r in canaries if r["stage"] == "A"]
    later = [r for r in canaries if r["stage"] != "A"]
    return a_can + a_rest + later + tail


def smoke_rows() -> list[dict]:
    # 17 cells. At duration 60s (warmup 5 + drain 5) plus about a minute of overhead
    # and one calibration cell, the wall clock on c7g.4xlarge is about 45-60 min.
    plan = [
        ("A", "pubsub", 200, 0, 0, 0),
        ("A", "streams", 200, 0, 0, 0),
        ("A", "kafka", 200, 0, 0, 0),
        ("A", "kafka-tuned", 200, 0, 0, 0),
        ("A", "kafka-acks", 200, 0, 0, 0),
        ("B", "kafka", 5000, 0, 0, 0),
        ("B", "kafka-tuned", 5000, 0, 0, 0),
        ("B", "kafka-acks", 5000, 0, 0, 0),
        ("B", "pubsub", 5000, 0, 0, 0),
        ("B", "streams", 5000, 0, 0, 0),
        ("C", "kafka-tuned", 2500, 0.01, 0, 0),
        ("C", "kafka-tuned", 2500, 1, 0, 0),
        ("C", "kafka", 2500, 0.01, 0, 0),
        ("C", "kafka", 5000, 0.01, 0, 0),
        ("C", "kafka-acks", 2500, 0.01, 0, 0),
        ("C", "pubsub", 5000, 1, 0, 0),
        ("C", "streams", 5000, 1, 1, 0),
    ]
    return [row(*spec) for spec in plan]


def kafka_rerun_rows() -> list[dict]:
    """40 cells: B at 3000/4000/5000, C at 2500, plus a few L5000 stress cells."""
    rows = []
    for broker in ("kafka", "kafka-tuned", "kafka-acks"):
        for load in (3000, 4000, 5000):
            for rep in range(2):
                rows.append(row("B", broker, load, 0, 0, rep))
    for broker in ("kafka", "kafka-tuned"):
        for nu in (0.01, 1):
            for kill in (0, 1):
                for rep in range(2):
                    rows.append(row("C", broker, 2500, nu, kill, rep))
    for nu in (0.01, 1):
        rows.append(row("C", "kafka-acks", 2500, nu, 0, 0))
    rows.append(row("C", "kafka", 5000, 0.01, 0, 0))
    rows.append(row("C", "kafka-tuned", 5000, 0.01, 0, 0))
    rows.append(row("C", "kafka", 5000, 1, 1, 0))
    rows.append(row("C", "kafka-acks", 5000, 0.01, 0, 0))
    return order_with_canaries(rows)


def factorial_rows(reps: int) -> list[dict]:
    rows = []
    for broker in ["pubsub", "streams", "kafka", "kafka-tuned"]:
        for rep in range(2):
            rows.append(row("A", broker, 200, 0, 0, rep))
    for broker in ["pubsub", "streams", "kafka", "kafka-tuned"]:
        for load in [1000, 2500, 5000]:
            for rep in range(reps):
                rows.append(row("B", broker, load, 0, 0, rep))
    for broker in ["pubsub", "streams", "kafka", "kafka-tuned"]:
        for load in [2500, 5000]:
            for nu in [0.01, 1]:
                for kill in [0, 1]:
                    for rep in range(reps):
                        rows.append(row("C", broker, load, nu, kill, rep))
    return order_with_canaries(rows)


CELL_ID = re.compile(
    r"^(?P<index>\d{3})-(?P<stage>[ABC])-(?P<broker>pubsub|streams|kafka|kafka-tuned|kafka-acks)"
    r"-L(?P<load>\d+)-nu(?P<nu>\d+(?:\.\d+)?)-k(?P<kill>[01])-r(?P<rep>\d+)$"
)

# Run 20261018T110200Z-c440945 (git c440945, 128 cells). Its kafka-tuned cells at L5000
# lost clients on first join and on every reconnect: a null lastSeenId replayed the whole
# partition through a fresh consumer group, and every catch-up opened another group
# (fixed in #16 and #17). Stage B L1000/L2500 and plain kafka lost no clients.
_RERUN_20261018_KAFKA_C = [
    "008-C-kafka-tuned-L5000-nu0.01-k0-r3", "010-C-kafka-tuned-L5000-nu1-k1-r3",
    "020-C-kafka-tuned-L5000-nu0.01-k1-r2", "022-C-kafka-tuned-L5000-nu0.01-k1-r4",
    "025-C-kafka-tuned-L5000-nu1-k0-r4", "028-C-kafka-tuned-L5000-nu0.01-k0-r4",
    "029-C-kafka-tuned-L5000-nu1-k0-r0", "032-C-kafka-tuned-L5000-nu1-k0-r1",
    "034-C-kafka-tuned-L5000-nu1-k1-r1", "036-C-kafka-tuned-L5000-nu1-k0-r3",
    "047-C-kafka-tuned-L5000-nu0.01-k0-r1", "069-C-kafka-tuned-L5000-nu0.01-k1-r1",
    "079-C-kafka-tuned-L5000-nu0.01-k1-r0", "080-C-kafka-tuned-L5000-nu1-k1-r2",
    "082-C-kafka-tuned-L5000-nu1-k0-r2", "097-C-kafka-tuned-L5000-nu0.01-k1-r3",
    "101-C-kafka-tuned-L5000-nu1-k1-r4", "106-C-kafka-tuned-L5000-nu0.01-k0-r0",
    "112-C-kafka-tuned-L5000-nu0.01-k0-r2", "125-C-kafka-tuned-L5000-nu1-k1-r0",
]
_RERUN_20261018_KAFKA_B = [
    "024-B-kafka-tuned-L5000-nu0-k0-r4", "043-B-kafka-tuned-L5000-nu0-k0-r1",
    "055-B-kafka-tuned-L5000-nu0-k0-r2", "083-B-kafka-tuned-L5000-nu0-k0-r3",
    "103-B-kafka-tuned-L5000-nu0-k0-r0",
]
# Same-code control for H3: the Streams cells of scenario C, so both sides of the
# TOST run on the same commit (the first-join fix in #16 touched every broker).
_RERUN_20261018_STREAMS_C = [
    "009-C-streams-L5000-nu1-k0-r2", "015-C-streams-L5000-nu0.01-k0-r1",
    "018-C-streams-L5000-nu1-k1-r2", "038-C-streams-L5000-nu0.01-k1-r2",
    "060-C-streams-L5000-nu0.01-k1-r4", "062-C-streams-L5000-nu1-k1-r0",
    "071-C-streams-L5000-nu0.01-k1-r1", "072-C-streams-L5000-nu0.01-k1-r0",
    "073-C-streams-L5000-nu1-k0-r0", "074-C-streams-L5000-nu0.01-k0-r2",
    "075-C-streams-L5000-nu1-k0-r3", "076-C-streams-L5000-nu0.01-k0-r4",
    "086-C-streams-L5000-nu0.01-k0-r3", "092-C-streams-L5000-nu1-k0-r4",
    "095-C-streams-L5000-nu0.01-k1-r3", "099-C-streams-L5000-nu1-k0-r1",
    "102-C-streams-L5000-nu1-k1-r1", "113-C-streams-L5000-nu1-k1-r4",
    "114-C-streams-L5000-nu1-k1-r3", "123-C-streams-L5000-nu0.01-k0-r0",
]
PRESETS: dict[str, list[str]] = {
    "rerun-20261018-kafka": _RERUN_20261018_KAFKA_C + _RERUN_20261018_KAFKA_B,
    "rerun-20261018-kafka-reliability": list(_RERUN_20261018_KAFKA_C),
    "rerun-20261018-streams-c": list(_RERUN_20261018_STREAMS_C),
}


def _number(text: str) -> int | float:
    value = float(text)
    return int(value) if value.is_integer() else value


def parse_cell_id(cell_id: str) -> dict:
    """Row for one run id of an earlier matrix. The id itself is kept as run_id."""
    match = CELL_ID.match(cell_id)
    if not match:
        raise ValueError(f"not a matrix run id: {cell_id!r}")
    load = int(match["load"])
    item = row(match["stage"], match["broker"], load, _number(match["nu"]), int(match["kill"]), int(match["rep"]))
    item["run_id"] = cell_id
    return item


def cells_rows(spec: str) -> list[dict]:
    """Cells named by run id or preset, deduplicated, risky cell of each broker x stage first."""
    tokens = [t for t in re.split(r"[\s,]+", spec.strip()) if t]
    if not tokens:
        raise ValueError("cells profile needs at least one run id or preset")
    ids: list[str] = []
    for token in tokens:
        for cell_id in PRESETS.get(token, [token]):
            if cell_id not in ids:
                ids.append(cell_id)
    return order_with_canaries([parse_cell_id(i) for i in ids])


def plan_for(argument: str, spec: str = "") -> tuple[str, list[dict]]:
    if argument in ("smoke", "--smoke"):
        return "smoke", smoke_rows()
    if argument in ("kafka-rerun", "--kafka-rerun"):
        return "kafka-rerun", kafka_rerun_rows()
    if argument in ("cells", "--cells"):
        return "cells", cells_rows(spec)
    return "factorial", factorial_rows(int(argument))


def write_matrix(rows: list[dict], path: str) -> None:
    for i, item in enumerate(rows):
        if item.get("run_id"):
            continue  # cells profile: keep the id of the run being replaced
        item["run_id"] = (
            f"{i:03d}-{item['stage']}-{item['broker']}-L{item['load']}"
            f"-nu{item['disconnects_per_min']}-k{item['gateway_kill']}-r{item['rep']}"
        )
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str]) -> None:
    args = [a for a in argv[1:] if a != "--dry-run"]
    dry_run = len(args) != len(argv) - 1
    try:
        label, rows = plan_for(args[0] if args else "5", " ".join(args[1:]))
    except ValueError as error:
        sys.exit(f"make_matrix: {error}")
    if dry_run:
        for item in rows:
            print(item.get("run_id") or f"{item['stage']}-{item['broker']}-L{item['load']}")
        print(f"{len(rows)} {label} runs (dry run, nothing written)")
        return
    write_matrix(rows, "configs/matrix.csv")
    print(f"{len(rows)} {label} runs written to configs/matrix.csv")


if __name__ == "__main__":
    main(sys.argv)
