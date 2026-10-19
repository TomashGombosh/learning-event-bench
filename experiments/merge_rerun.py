#!/usr/bin/env python3
"""Overlay a profile=cells rerun on its base run.

Usage: merge_rerun.py BASE_DIR RERUN_DIR BASE_RUN_ID RERUN_RUN_ID

Every run id in RERUN_DIR/matrix.csv that finished in the rerun (it has a shard summary
and no cell-failed.txt) replaces BASE_DIR/<run id>. Ids that are not in the base matrix,
or that did not finish in the rerun, are reported and left alone, so a half-finished
rerun never deletes good base data. BASE_DIR/matrix.csv stays the base matrix.
"""
from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path


def finished(cell: Path) -> bool:
    return cell.is_dir() and not (cell / "cell-failed.txt").exists() and any(cell.glob("shard-*/summary.json"))


def merge(base: Path, rerun: Path, base_id: str, rerun_id: str) -> dict:
    base_ids = {r["run_id"] for r in csv.DictReader((base / "matrix.csv").open())}
    rerun_ids = [r["run_id"] for r in csv.DictReader((rerun / "matrix.csv").open())]
    replaced, unfinished, unknown = [], [], []
    for run_id in rerun_ids:
        if run_id not in base_ids:
            unknown.append(run_id)
        elif not finished(rerun / run_id):
            unfinished.append(run_id)
        else:
            shutil.rmtree(base / run_id, ignore_errors=True)
            shutil.copytree(rerun / run_id, base / run_id)
            replaced.append(run_id)
    for name in ("run.log", "validity.json", "run-metadata.json"):
        if (rerun / name).exists():
            shutil.copy2(rerun / name, base / f"rerun-{name}")
    report = {"base_run_id": base_id, "rerun_run_id": rerun_id, "replaced": replaced,
              "unfinished_in_rerun": unfinished, "not_in_base_matrix": unknown}
    (base / "merged-from.json").write_text(json.dumps(report, indent=1) + "\n")
    return report


def main(argv: list[str]) -> None:
    if len(argv) != 5:
        sys.exit(__doc__)
    report = merge(Path(argv[1]), Path(argv[2]), argv[3], argv[4])
    print(f"merged {len(report['replaced'])} cells from {argv[4]} into {argv[3]}; "
          f"unfinished={len(report['unfinished_in_rerun'])} unknown={len(report['not_in_base_matrix'])}")
    for key in ("unfinished_in_rerun", "not_in_base_matrix"):
        for run_id in report[key]:
            print(f"WARNING {key}: {run_id}")


if __name__ == "__main__":
    main(sys.argv)
