#!/usr/bin/env python3
"""Generates the randomized run plan (experiments/configs/matrix.csv).

Stage A (calibration): low load, every broker, 2 reps.
Stage B (latency, H1/H4): broker{4} x total load{1000,2500,5000 ev/s} x REPS, n=20, no faults.
Stage C (reliability, H2/H3/H5): broker{3} x disconnects{0.01,1 per min} x gateway kill{no,yes} x REPS,
         load 5000 ev/s (50 rooms x 20 clients x 5 ev/s).

`make_matrix.py smoke` (or `--smoke`) writes a handful of cells. bench_reps=1 on the
instance selects this plan. REPS=1 of the full factorial is still ~32 cells because
stage A is fixed at 2 reps and B/C stay a full cross product, which is why a "smoke"
run took hours.
"""
import csv, random, sys

SEED = 20261101
CLIENTS, RATE = 20, 5.0

def rooms_for(load):  # total events/s = rooms * clients * rate
    return int(load / (CLIENTS * RATE))

def row(stage, broker, load, nu, kill, rep):
    return dict(stage=stage, broker=broker, load=load, rooms=rooms_for(load), clients=CLIENTS, rate=RATE,
                disconnects_per_min=nu, gateway_kill=kill, rep=rep)

def smoke_rows():
    # One calibration cell per broker, the L5000 latency cell that went nonsensical,
    # and the streams + gateway-kill cell that hung.
    plan = [
        ("A", "pubsub", 200, 0, 0, 0),
        ("A", "streams", 200, 0, 0, 0),
        ("A", "kafka", 200, 0, 0, 0),
        ("A", "kafka-tuned", 200, 0, 0, 0),
        ("B", "pubsub", 5000, 0, 0, 0),
        ("C", "streams", 5000, 1, 1, 0),
    ]
    return [row(*spec) for spec in plan]

def factorial_rows(reps):
    rows = []
    for broker in ["pubsub", "streams", "kafka", "kafka-tuned"]:
        for rep in range(2):
            rows.append(row("A", broker, 200, 0, 0, rep))
    for broker in ["pubsub", "streams", "kafka", "kafka-tuned"]:
        for load in [1000, 2500, 5000]:
            for rep in range(reps):
                rows.append(row("B", broker, load, 0, 0, rep))
    for broker in ["pubsub", "streams", "kafka-tuned"]:
        for nu in [0.01, 1]:
            for kill in [0, 1]:
                for rep in range(reps):
                    rows.append(row("C", broker, 5000, nu, kill, rep))
    random.Random(SEED).shuffle(rows)  # randomized run order (calibration first)
    rows.sort(key=lambda r: r["stage"] != "A")
    return rows

smoke = len(sys.argv) > 1 and sys.argv[1] in ("smoke", "--smoke")
rows = smoke_rows() if smoke else factorial_rows(int(sys.argv[1]) if len(sys.argv) > 1 else 5)
for i, r in enumerate(rows):
    r["run_id"] = f"{i:03d}-{r['stage']}-{r['broker']}-L{r['load']}-nu{r['disconnects_per_min']}-k{r['gateway_kill']}-r{r['rep']}"
with open("configs/matrix.csv", "w", newline="") as f:
    # Default csv lineterminator is \r\n. bash `read` then keeps a trailing CR on run_id.
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), lineterminator="\n")
    w.writeheader(); w.writerows(rows)
label = "smoke" if smoke else "factorial"
print(f"{len(rows)} {label} runs written to configs/matrix.csv")
