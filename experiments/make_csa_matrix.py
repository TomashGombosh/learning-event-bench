#!/usr/bin/env python3
"""CSA matrix: 6 alternatives × 3 scenarios × 2 fault levels × 5 reps = 180 cells.

Smoke is the same cross on the whiteboard profile, one rep: 12 cells.
The risky cell of each scheme × transport × scenario (nu = 1/min and a gateway
kill) is first, so fail-fast hits it before the calm cells.

The plan is refused when it does not match csa_profiles.py, when run ids
collide, or when experiments/run_matrix.sh no longer emits the SATURATED flag.
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

from csa_profiles import (
    BROKERS,
    CSA_SEED,
    FAULTS,
    PROFILES,
    REPS,
    SCHEMES,
    SMOKE_PROFILE,
    SMOKE_REPS,
)
from make_matrix import write_matrix

ROOT = Path(__file__).resolve().parent
SATURATED_TOKEN = 'flags.append("SATURATED")'


def require_saturated_flag(path: Path) -> None:
    text = path.read_text()
    if SATURATED_TOKEN not in text or "SATURATED_VALID" not in text:
        raise ValueError(f"SATURATED flag missing from {path.name}; refusing to write the CSA plan")


def _rows(reps: int, profiles: dict) -> list[dict]:
    rows = []
    for scheme in SCHEMES:
        for broker in BROKERS:
            for stage, profile in profiles.items():
                for fault in FAULTS:
                    for rep in range(reps):
                        rows.append({
                            "stage": stage,
                            "broker": broker,
                            "load": profile["load"],
                            "rooms": profile["rooms"],
                            "clients": profile["clients"],
                            "rate": profile["rate"],
                            "disconnects_per_min": fault["disconnects_per_min"],
                            "gateway_kill": fault["gateway_kill"],
                            "rep": rep,
                            "scheme": scheme,
                        })
    return _order(rows)


def _order(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple, list] = {}
    for item in rows:
        grouped.setdefault((item["stage"], item["scheme"], item["broker"]), []).append(item)
    canaries, rest = [], []
    for items in grouped.values():
        items.sort(key=lambda r: (r["disconnects_per_min"], r["gateway_kill"], -r["rep"]), reverse=True)
        canaries.append(items[0])
        rest.extend(items[1:])
    canaries.sort(key=lambda r: (r["stage"], r["scheme"], r["broker"]))
    random.Random(CSA_SEED).shuffle(rest)
    ordered = canaries + rest
    for index, item in enumerate(ordered):
        item["run_id"] = (
            f"{index:03d}-{item['stage']}-{item['scheme']}-{item['broker']}"
            f"-L{item['load']}-nu{item['disconnects_per_min']}-k{item['gateway_kill']}-r{item['rep']}"
        )
    return ordered


def build(mode: str) -> list[dict]:
    if mode in ("smoke", "--smoke"):
        return _rows(SMOKE_REPS, {SMOKE_PROFILE: PROFILES[SMOKE_PROFILE]})
    if mode in ("full", "5", "--full"):
        return _rows(REPS, PROFILES)
    raise ValueError(f"unknown CSA mode {mode!r} (smoke or full)")


def check_plan(rows: list[dict], mode: str) -> None:
    if not rows:
        raise ValueError("empty plan")
    alternatives = {(r["scheme"], r["broker"]) for r in rows}
    if alternatives != {(s, b) for s in SCHEMES for b in BROKERS}:
        raise ValueError(f"expected {len(SCHEMES) * len(BROKERS)} alternatives, got {sorted(alternatives)}")
    if any(r.get("scheme") not in SCHEMES for r in rows):
        raise ValueError("scheme column missing or unknown")
    ids = [r["run_id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("run ids are not unique")
    expected_profiles = {SMOKE_PROFILE: PROFILES[SMOKE_PROFILE]} if mode in ("smoke", "--smoke") else PROFILES
    for stage, profile in expected_profiles.items():
        product = profile["rooms"] * profile["clients"] * profile["rate"]
        if product != profile["load"]:
            raise ValueError(f"{stage} load {profile['load']} != rooms*clients*rate ({product})")
        cells = [r for r in rows if r["stage"] == stage]
        if not cells:
            raise ValueError(f"profile {stage} produced no cells")
        for cell in cells:
            for key in ("clients", "rate", "rooms", "load"):
                if cell[{"clients": "clients", "rate": "rate", "rooms": "rooms", "load": "load"}[key]] != profile[key]:
                    raise ValueError(f"{stage} cell does not match the profile config")
    reps = SMOKE_REPS if mode in ("smoke", "--smoke") else REPS
    expected = len(SCHEMES) * len(BROKERS) * len(expected_profiles) * len(FAULTS) * reps
    if len(rows) != expected:
        raise ValueError(f"expected {expected} cells, got {len(rows)}")
    seen: dict[tuple, dict] = {}
    for item in rows:
        seen.setdefault((item["stage"], item["scheme"], item["broker"]), item)
    for key, first in seen.items():
        if (first["disconnects_per_min"], first["gateway_kill"]) != (1, 1):
            raise ValueError(f"canary for {key} is not nu=1 gateway_kill=1")
    require_saturated_flag(ROOT / "run_matrix.sh")


def main(argv: list[str]) -> None:
    args = [a for a in argv[1:] if a != "--dry-run"]
    dry_run = "--dry-run" in argv[1:]
    mode = args[0] if args else "full"
    try:
        rows = build(mode)
        check_plan(rows, mode)
    except ValueError as error:
        sys.exit(f"make_csa_matrix: {error}")
    label = "smoke" if mode in ("smoke", "--smoke") else "full"
    if dry_run:
        for item in rows:
            print(item["run_id"])
        print(f"{len(rows)} CSA {label} runs (dry run, nothing written)")
        return
    name = "csa-smoke.csv" if label == "smoke" else "csa-matrix.csv"
    path = ROOT / "configs" / name
    write_matrix(rows, str(path))
    print(f"{len(rows)} CSA {label} runs written to {path}")
    print("VALIDITY_FAIL_FAST=1 for CSA smoke and full (see experiments/run_csa.sh)")


if __name__ == "__main__":
    main(sys.argv)
