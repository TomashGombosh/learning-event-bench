"""bench-up, run-bench, and Terraform accept the same profile names.

select_matrix.py is the instance entry point: CSA plans come from
make_csa_matrix.py (Yjs and ShareDB), everything else from make_matrix.py.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import select_matrix  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github" / "workflows" / "bench-up.yml"
RUN_BENCH = ROOT / "infra" / "instance" / "run-bench.sh"
VARIABLES = ROOT / "infra" / "terraform" / "variables.tf"


def _workflow_options() -> list[str]:
    text = WORKFLOW.read_text()
    block = re.search(r"options:\n((?:          - .+\n)+)", text)
    assert block, "bench-up.yml profile options not found"
    return re.findall(r"^          - (\S+)$", block.group(1), re.M)


def _terraform_profiles() -> list[str]:
    text = VARIABLES.read_text()
    block = re.search(r'variable "bench_profile".*?condition\s*=\s*contains\(\[(.*?)\], var\.bench_profile\)', text, re.S)
    assert block, "terraform bench_profile validation not found"
    return re.findall(r'"([^"]+)"', block.group(1))


def _run_bench_profile_arms() -> set[str]:
    text = RUN_BENCH.read_text()
    # Settings case is the first `case "$profile" in` in the file.
    start = text.find('case "$profile" in')
    assert start >= 0
    end = text.find("\nesac\n", start)
    body = text[start:end]
    arms = set(re.findall(r"^\s+([A-Za-z0-9-]+)\)", body, re.M))
    arms.discard("*")
    return arms


def test_profile_lists_match_across_workflow_terraform_and_run_bench():
    expected = list(select_matrix.BENCH_PROFILES)
    assert _workflow_options() == expected
    assert _terraform_profiles() == expected
    assert _run_bench_profile_arms() == set(expected)


def test_unknown_profile_is_rejected_and_does_not_become_smoke():
    with pytest.raises(ValueError, match="unknown bench profile"):
        select_matrix.select("not-a-profile")
    run_bench = RUN_BENCH.read_text()
    workflow = WORKFLOW.read_text()
    assert "unknown bench profile" in run_bench
    # The old fallback turned any typo into the 17-cell Yjs smoke.
    assert re.search(r"^\s+\*\)\s*\n\s+profile=smoke", run_bench, re.M) is None
    assert 'PROFILE=smoke' not in workflow
    assert "unknown bench profile" in workflow


def test_run_bench_builds_every_matrix_through_select_matrix():
    text = RUN_BENCH.read_text()
    assert "select_matrix.py" in text
    assert "make_csa_matrix.py" in text or "csa-smoke" in text
    # A typo must not silently start the Yjs factorial via make_matrix.py $REPS.
    assert 'make_matrix.py "${BENCH_REPS}"' not in text


def test_select_matrix_csa_smoke_is_12_cells_with_both_schemes(tmp_path: Path):
    label, rows = select_matrix.select("csa-smoke")
    assert label == "csa-smoke"
    assert len(rows) == 12
    assert {r["scheme"] for r in rows} == {"yjs", "sharedb"}
    assert {r["broker"] for r in rows} == {"pubsub", "streams", "kafka-tuned"}
    assert {r["stage"] for r in rows} == {"whiteboard"}
    out = tmp_path / "matrix.csv"
    select_matrix.make_matrix.write_matrix(rows, str(out))
    text = out.read_text()
    assert "sharedb" in text and "yjs" in text
    header = text.splitlines()[0].split(",")
    assert header[-1] == "scheme"


def test_select_matrix_csa_full_is_180_cells():
    label, rows = select_matrix.select("csa-full")
    assert label == "csa-full"
    assert len(rows) == 180
    assert {r["scheme"] for r in rows} == {"yjs", "sharedb"}
    assert {r["stage"] for r in rows} == {"whiteboard", "quiz", "class-chat"}


def test_select_matrix_yjs_smoke_is_not_the_csa_plan():
    label, rows = select_matrix.select("smoke")
    assert label == "smoke"
    assert len(rows) == 17
    assert {r["scheme"] for r in rows} == {"yjs"}


def test_select_matrix_cli_dry_run_does_not_write(tmp_path: Path):
    out = tmp_path / "matrix.csv"
    completed = subprocess.run(
        [sys.executable, str(ROOT / "experiments" / "select_matrix.py"), "csa-smoke", "--dry-run", "--out", str(out)],
        check=False, capture_output=True, text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "12 csa-smoke runs (dry run, nothing written)" in completed.stdout
    assert not out.exists()
    completed = subprocess.run(
        [sys.executable, str(ROOT / "experiments" / "select_matrix.py"), "mmc-smoke", "--dry-run"],
        check=False, capture_output=True, text=True, cwd=str(ROOT / "experiments"),
    )
    assert completed.returncode == 0, completed.stderr
    assert "6 mmc-smoke runs (dry run, nothing written)" in completed.stdout


def test_workflow_dry_runs_csa_and_mmc_before_apply():
    text = WORKFLOW.read_text()
    assert "Validate the CSA matrix" in text
    assert "select_matrix.py" in text
    assert "inputs.profile == 'csa-smoke' || inputs.profile == 'csa-full'" in text
    # Terraform apply must come after the dry-run steps.
    assert text.find("Validate the CSA matrix") < text.find("Terraform apply")
    assert text.find("Validate the MMC matrix") < text.find("Terraform apply")
