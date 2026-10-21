#!/usr/bin/env python3
"""Matrices for the MMC overload / finite-window article.

Seed MMC_SEED is not make_matrix.SEED (20261101). No faults: nu = 0, kill = 0.

E1 saturation: pubsub, streams, kafka, kafka-tuned
  x L in {200, 1000, 2000, 3000, 3500, 4000, 5000, 6000} x 3 reps, T = 240 s.
  4 x 8 x 3 = 96 cells.

E2 window dependence: two overload loads, 5000 and 6000 (at and above the
plain-kafka collapse in the 20261019 runs). kafka and kafka-tuned at both
loads; streams only at 6000. T in {60, 120, 240, 480} x 3 reps.
  (2 + 2) x 2 x 4 x 3 + 1 x 4 x 3 = 60 cells.

Smoke: 6 cells, about 40 min on c7g.4xlarge. bench-up leaves fail-fast on.
"""
from __future__ import annotations

import sys

import make_matrix

MMC_SEED = 20261021
TRANSPORTS = ("pubsub", "streams", "kafka", "kafka-tuned")
E1_LOADS = (200, 1000, 2000, 3000, 3500, 4000, 5000, 6000)
# At and above the load where plain kafka stopped delivering in the 20261019 runs.
OVERLOAD_LOADS = (5000, 6000)
WINDOWS = (60, 120, 240, 480)
REPS = 3
E1_DURATION = 240


def e1_rows() -> list[dict]:
    rows = []
    for broker in TRANSPORTS:
        for load in E1_LOADS:
            for rep in range(REPS):
                rows.append(make_matrix.row("B", broker, load, 0, 0, rep, E1_DURATION))
    return make_matrix.order_with_canaries(rows, MMC_SEED)


def e2_rows() -> list[dict]:
    rows = []
    plan = (
        ("kafka", OVERLOAD_LOADS),
        ("kafka-tuned", OVERLOAD_LOADS),
        ("streams", (6000,)),
    )
    for broker, loads in plan:
        for load in loads:
            for duration in WINDOWS:
                for rep in range(REPS):
                    rows.append(make_matrix.row("B", broker, load, 0, 0, rep, duration))
    return make_matrix.order_with_canaries(rows, MMC_SEED)


def smoke_rows() -> list[dict]:
    """Six cells: one quiet cell per transport, plus the L6000 canaries and one short window.

    Five cells at T=240 and one at T=60. With warmup/drain and about a minute of
    overhead, plus calibration, the wall clock is about 40 min.
    """
    plan = [
        ("pubsub", 200, 240, 0),
        ("streams", 200, 240, 0),
        ("kafka", 200, 240, 0),
        ("kafka-tuned", 6000, 240, 0),
        ("kafka", 6000, 240, 0),
        ("streams", 6000, 60, 0),
    ]
    rows = [make_matrix.row("B", broker, load, 0, 0, rep, duration) for broker, load, duration, rep in plan]
    return make_matrix.order_with_canaries(rows, MMC_SEED)


def rows_for(mode: str) -> list[dict]:
    if mode in ("mmc-smoke", "smoke"):
        return smoke_rows()
    if mode in ("mmc-e1", "e1"):
        return e1_rows()
    if mode in ("mmc-e2", "e2"):
        return e2_rows()
    raise ValueError(f"unknown MMC matrix {mode!r}")


def main(argv: list[str]) -> None:
    mode = argv[1] if len(argv) > 1 else "e1"
    dry_run = "--dry-run" in argv
    try:
        rows = rows_for(mode)
    except ValueError as error:
        sys.exit(f"make_mmc_matrix: {error}")
    label = mode if mode.startswith("mmc-") else f"mmc-{mode}"
    if dry_run:
        for item in rows:
            print(f"{item['stage']}-{item['broker']}-L{item['load']}-T{item['duration_s']}")
        print(f"{len(rows)} {label} runs (dry run, nothing written)")
        return
    make_matrix.write_matrix(rows, "configs/matrix.csv")
    print(f"{len(rows)} {label} runs written to configs/matrix.csv")


if __name__ == "__main__":
    main(sys.argv)
