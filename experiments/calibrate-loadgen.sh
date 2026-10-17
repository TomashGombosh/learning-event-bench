#!/usr/bin/env bash
# Offers L5000 for 30s. Exits 1 when any shard (or the total) delivers under 95% of
# the offered send rate, so a generator-bound host does not spend hours on the matrix.
# run-bench.sh turns that exit into an S3 upload and power-off.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
rm -rf results/calibration
cat > results/calibration.csv <<'EOF'
stage,broker,load,rooms,clients,rate,disconnects_per_min,gateway_kill,rep,run_id
B,pubsub,5000,50,20,5.0,0,0,0,calibration
EOF
echo "calibration: L5000 for 30s on loadgen cpuset=${CPUSET_LOADGEN:-unset} cores=${LOADGEN_CPUS:-unset} shards=${SHARDS:-unset}"
DURATION_S=30 WARMUP_S=2 DRAIN_S=2 ./experiments/run_matrix.sh results/calibration.csv
python3 - <<'PY'
import json, os, sys
from pathlib import Path
shards = sorted(Path("results/calibration").glob("shard-*/summary.json"))
if not shards:
    msg = "CALIBRATION FAILED: load generator produced no summary.json. Aborting before the matrix."
    print(msg, file=sys.stderr)
    Path("results/calibration").mkdir(parents=True, exist_ok=True)
    Path("results/calibration/CALIBRATION_FAILED.txt").write_text(msg + "\n")
    sys.exit(1)
total_offered = 0.0
total_achieved = 0.0
lines = []
worst = 1.0
for path in shards:
    summary = json.loads(path.read_text())
    load = summary["load"]
    offered = float(load["offeredPerSec"])
    achieved = float(load["achievedSendPerSec"])
    ratio = achieved / offered if offered else 0.0
    worst = min(worst, ratio)
    total_offered += offered
    total_achieved += achieved
    lines.append(
        f"  {path.parent.name}: achieved {achieved:.1f}/s vs offered {offered:.1f}/s "
        f"({ratio:.1%}), schedule-lag p50 {float(load['scheduleLagP50Ms']):.1f}ms"
    )
ratio = total_achieved / total_offered if total_offered else 0.0
cpus = os.environ.get("CPUSET_LOADGEN", "unset")
cores = os.environ.get("LOADGEN_CPUS", "unset")
shards_n = os.environ.get("SHARDS", "unset")
header = (
    f"calibration L5000: achieved {total_achieved:.1f}/s vs offered {total_offered:.1f}/s "
    f"({ratio:.1%}); loadgen cpuset {cpus} ({cores} cores, {shards_n} shards)"
)
print(header)
print("\n".join(lines))
if ratio < 0.95 or worst < 0.95:
    msg = (
        f"CALIBRATION FAILED: {header}. Need >= 95% of offered on every shard. "
        "Aborting before the matrix so this host does not burn hours of unusable cells."
    )
    print(msg, file=sys.stderr)
    Path("results/calibration/CALIBRATION_FAILED.txt").write_text(msg + "\n" + "\n".join(lines) + "\n")
    sys.exit(1)
print("CALIBRATION OK")
PY
