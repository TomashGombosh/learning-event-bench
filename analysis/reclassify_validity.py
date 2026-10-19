#!/usr/bin/env python3
"""Recompute validity statuses from shard summaries already on disk.

Writes a new file (default ``<results>/validity-reclassified.json``) and prints
one VALIDITY line per cell. Does not overwrite ``validity.json``. When that
file is present, also prints ``old -> new`` for every status that changed.

Usage:
  python analysis/reclassify_validity.py results
  python analysis/reclassify_validity.py results --matrix experiments/configs/matrix.csv
  python analysis/reclassify_validity.py results --out results/validity-reclassified.json
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

_EXPERIMENTS = Path(__file__).resolve().parents[1] / "experiments"
if str(_EXPERIMENTS) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENTS))

from validity import (  # noqa: E402
    assess_cell,
    empty_state,
    load_observation,
    load_state,
    record_cell,
    summary_line,
)

# 003-C-kafka-L5000-nu1-k1-r0, and brokers that themselves contain hyphens.
RUN_ID_RE = re.compile(
    r"^(?P<index>\d+)-(?P<stage>[A-Za-z])-(?P<broker>.+)"
    r"-L(?P<load>\d+(?:\.\d+)?)-nu(?P<nu>[0-9.]+)-k(?P<kill>\d+)-r(?P<rep>\d+)$"
)


def _cell(row: dict, *names: str, default: float = 0) -> float:
    for name in names:
        raw = row.get(name, "")
        if raw is None or raw == "":
            continue
        return float(raw)
    return default


def load_matrix(path: Path | None) -> dict[str, dict]:
    if path is None or not path.is_file():
        return {}
    with path.open(newline="") as handle:
        return {row["run_id"]: row for row in csv.DictReader(handle) if row.get("run_id")}


def discover_runs(results: Path) -> list[str]:
    found = []
    if not results.is_dir():
        return found
    for summary in sorted(results.glob("*/shard-*/summary.json")):
        run_id = summary.parents[1].name
        if run_id not in found:
            found.append(run_id)
    return found


def describe(run_id: str, row: dict | None, shards: int) -> dict:
    parsed = RUN_ID_RE.match(run_id)
    fields = parsed.groupdict() if parsed else {}
    source = row or {}
    stage = source.get("stage") or fields.get("stage") or ""
    broker = source.get("broker") or fields.get("broker") or ""
    load = _cell(source, "load", default=_cell(fields, "load"))
    nu = _cell(source, "disconnects_per_min", "nu", default=_cell(fields, "nu"))
    kill = int(_cell(source, "gateway_kill", "kill", default=_cell(fields, "kill")))
    rooms = int(_cell(source, "rooms", default=shards))
    clients = int(_cell(source, "clients", default=0))
    if rooms <= 0:
        rooms = shards
    return {
        "stage": stage,
        "broker": broker,
        "load": load,
        "nu": nu,
        "kill": kill,
        "rooms": rooms,
        "clients": clients,
        "shards": shards,
    }


def reclassify(results: Path, matrix: Path | None = None, out: Path | None = None) -> dict:
    """Assess every run that already has a shard summary. Returns the new state."""
    results = results.resolve()
    destination = (out if out is not None else results / "validity-reclassified.json").resolve()
    original = results / "validity.json"
    if destination == original:
        raise SystemExit("refusing to overwrite validity.json; pass a different --out")

    plan = load_matrix(matrix)
    state = empty_state()
    lines: list[str] = []
    for run_id in discover_runs(results):
        shards = len(list((results / run_id).glob("shard-*/summary.json")))
        meta = describe(run_id, plan.get(run_id), shards)
        if not meta["stage"] or not meta["broker"]:
            print(f"RECLASS skip {run_id}: run id does not match the matrix or the cell pattern", file=sys.stderr)
            continue
        obs = load_observation(results, run_id=run_id, **meta)
        verdict = assess_cell(obs)
        record_cell(state, verdict, fail_fast=False)
        lines.append(verdict.line())

    previous = load_state(original) if original.is_file() else None
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(state, indent=2) + "\n")
    for line in lines:
        print(line)
    if previous is not None:
        old = {cell.get("run_id"): cell for cell in previous.get("cells") or []}
        for cell in state["cells"]:
            prior = old.get(cell["run_id"])
            if prior is None or prior.get("status") == cell.get("status"):
                continue
            print(f"RECLASS {cell['run_id']} {prior.get('status')} -> {cell.get('status')}")
    print(summary_line(state))
    return state


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recompute validity flags from existing summaries")
    parser.add_argument("results", type=Path)
    parser.add_argument("--matrix", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)
    reclassify(args.results, args.matrix, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
