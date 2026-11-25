#!/usr/bin/env python3
"""Write experiments/configs/matrix.csv for a bench-up / run-bench profile.

CSA profiles call make_csa_matrix.py (Yjs and ShareDB crossed with the CSA
transports). MMC and the original factorial go through make_matrix.py.
The output is always configs/matrix.csv, which run-bench.sh executes.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import make_csa_matrix
import make_matrix

ROOT = Path(__file__).resolve().parent
DEFAULT_OUT = ROOT / "configs" / "matrix.csv"

# Canonical list. bench-up.yml, run-bench.sh, and the Terraform
# bench_profile validation must accept exactly these names.
BENCH_PROFILES = (
    "smoke",
    "mmc-smoke",
    "mmc-e1",
    "mmc-e2",
    "csa-smoke",
    "csa-full",
    "kafka-rerun",
    "cells",
    "full",
)


def select(profile: str, cells: str = "", reps: str = "5") -> tuple[str, list[dict]]:
    """Return (label, rows) for one profile. Raises ValueError on a typo."""
    if profile == "csa-smoke":
        rows = make_csa_matrix.build("smoke")
        make_csa_matrix.check_plan(rows, "smoke")
        return "csa-smoke", rows
    if profile == "csa-full":
        rows = make_csa_matrix.build("full")
        make_csa_matrix.check_plan(rows, "full")
        return "csa-full", rows
    if profile == "full":
        return make_matrix.plan_for(str(reps))
    if profile == "cells":
        return make_matrix.plan_for("cells", cells)
    if profile in ("smoke", "kafka-rerun", "mmc-smoke", "mmc-e1", "mmc-e2"):
        return make_matrix.plan_for(profile)
    raise ValueError(
        f"unknown bench profile {profile!r}; expected one of {', '.join(BENCH_PROFILES)}"
    )


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description="Build the matrix.csv for a bench profile")
    parser.add_argument("profile", help="bench-up / run-bench profile name")
    parser.add_argument("--cells", default="", help="profile=cells only: run ids or presets")
    parser.add_argument("--reps", default="5", help="profile=full only: factorial repetitions")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv[1:])
    try:
        label, rows = select(args.profile, cells=args.cells, reps=args.reps)
    except ValueError as error:
        sys.exit(f"select_matrix: {error}")
    if args.dry_run:
        for item in rows:
            print(item.get("run_id") or f"{item.get('stage')}-{item.get('broker')}-L{item.get('load')}")
        print(f"{len(rows)} {label} runs (dry run, nothing written)")
        return
    args.out.parent.mkdir(parents=True, exist_ok=True)
    make_matrix.write_matrix(rows, str(args.out))
    print(f"{len(rows)} {label} runs written to {args.out}")


if __name__ == "__main__":
    main(sys.argv)
