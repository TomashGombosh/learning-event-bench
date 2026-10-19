#!/usr/bin/env python3
"""Validity gate for one cell, or the PASS/FAIL table for the run.

Lines go to stdout. On the bench host that stream is /var/log/leb/bench.log, which the
CloudWatch agent ships to log group /leb/bench (stream <run_id>/bench.log).

Exit 0: cell passed, or --summary.
Exit 1: cell failed and this is not the fail-fast abort.
Exit 42: fail-fast. The first cell of this broker x scenario group failed.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from validity import (
    assess_cell,
    load_observation,
    load_state,
    record_cell,
    summary_line,
    table,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Check bench cell validity")
    parser.add_argument("--results", default="results")
    parser.add_argument("--cell", action="store_true")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--aborted", action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--catchup-max", type=int, default=10)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--stage", default="")
    parser.add_argument("--broker", default="")
    parser.add_argument("--load", type=float, default=0)
    parser.add_argument("--nu", type=float, default=0)
    parser.add_argument("--kill", type=int, default=0)
    parser.add_argument("--rooms", type=int, default=0)
    parser.add_argument("--clients", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    args = parser.parse_args()

    results = Path(args.results)
    results.mkdir(parents=True, exist_ok=True)
    path = results / "validity.json"
    state = load_state(path)

    if args.summary or args.aborted:
        if args.aborted:
            state["aborted"] = True
            state.setdefault("reason", "validity gate failed")
        path.write_text(json.dumps(state, indent=2) + "\n")
        print(table(state))
        print(summary_line(state))
        return 0

    if not args.cell:
        parser.error("pass --cell or --summary")

    obs = load_observation(
        results,
        run_id=args.run_id,
        stage=args.stage,
        broker=args.broker,
        load=args.load,
        nu=args.nu,
        kill=args.kill,
        rooms=args.rooms,
        clients=args.clients,
        shards=args.shards,
    )
    verdict = assess_cell(obs, catchup_error_max=args.catchup_max)
    code = record_cell(state, verdict, fail_fast=args.fail_fast)
    path.write_text(json.dumps(state, indent=2) + "\n")
    print(verdict.line())
    if code == 42:
        print(f"VALIDITY_ABORT group={verdict.stage}|{verdict.broker} cell={verdict.run_id} reason={state['reason']}")
    return code


if __name__ == "__main__":
    sys.exit(main())
