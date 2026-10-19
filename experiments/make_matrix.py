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
  <reps>        full factorial. reps=5 is the long run.

The first cell of each broker x scenario is the risky one (highest load, faults on).
Full and kafka-rerun runs abort if that cell fails the validity gate.
"""
from __future__ import annotations

import csv
import random
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


def plan_for(argument: str) -> tuple[str, list[dict]]:
    if argument in ("smoke", "--smoke"):
        return "smoke", smoke_rows()
    if argument in ("kafka-rerun", "--kafka-rerun"):
        return "kafka-rerun", kafka_rerun_rows()
    return "factorial", factorial_rows(int(argument))


def write_matrix(rows: list[dict], path: str) -> None:
    for i, item in enumerate(rows):
        item["run_id"] = (
            f"{i:03d}-{item['stage']}-{item['broker']}-L{item['load']}"
            f"-nu{item['disconnects_per_min']}-k{item['gateway_kill']}-r{item['rep']}"
        )
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str]) -> None:
    label, rows = plan_for(argv[1] if len(argv) > 1 else "5")
    write_matrix(rows, "configs/matrix.csv")
    print(f"{len(rows)} {label} runs written to configs/matrix.csv")


if __name__ == "__main__":
    main(sys.argv)
