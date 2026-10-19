"""merge_rerun overlays finished rerun cells on the base run and leaves the rest alone."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import merge_rerun  # noqa: E402

HEADER = "stage,broker,load,rooms,clients,rate,disconnects_per_min,gateway_kill,rep,run_id\n"


def _cell(root: Path, run_id: str, tag: str, failed: bool = False) -> None:
    shard = root / run_id / "shard-0"
    shard.mkdir(parents=True)
    (shard / "summary.json").write_text(json.dumps({"tag": tag}))
    if failed:
        (root / run_id / "cell-failed.txt").write_text("timeout")


def test_merge_replaces_only_finished_known_cells(tmp_path):
    base, rerun = tmp_path / "base", tmp_path / "rerun"
    a, b, c = "001-C-kafka-tuned-L5000-nu1-k0-r0", "002-C-kafka-tuned-L5000-nu1-k0-r1", "003-C-streams-L5000-nu1-k0-r0"
    base.mkdir(); rerun.mkdir()
    (base / "matrix.csv").write_text(HEADER + "".join(f"C,x,5000,50,20,5.0,1,0,0,{i}\n" for i in (a, b, c)))
    for i in (a, b, c):
        _cell(base, i, "old")
    (rerun / "matrix.csv").write_text(HEADER + "".join(f"C,x,5000,50,20,5.0,1,0,0,{i}\n" for i in (a, b, "999-C-kafka-L1-nu1-k0-r0")))
    _cell(rerun, a, "new")
    _cell(rerun, b, "new", failed=True)
    report = merge_rerun.merge(base, rerun, "BASE", "RERUN")
    assert report["replaced"] == [a]
    assert report["unfinished_in_rerun"] == [b]
    assert report["not_in_base_matrix"] == ["999-C-kafka-L1-nu1-k0-r0"]
    assert json.loads((base / a / "shard-0" / "summary.json").read_text())["tag"] == "new"
    assert json.loads((base / b / "shard-0" / "summary.json").read_text())["tag"] == "old"
    assert json.loads((base / c / "shard-0" / "summary.json").read_text())["tag"] == "old"
    assert (base / "merged-from.json").exists()
