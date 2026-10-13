#!/usr/bin/env python3
"""Generates the randomized run plan (experiments/configs/matrix.csv).

Stage A (calibration): low load, every broker, 2 reps.
Stage B (latency, H1/H4): broker{4} x total load{1000,2500,5000 ev/s} x 5 reps, n=20, no faults.
Stage C (reliability, H2/H3/H5): broker{3} x disconnects{0.01,1 per min} x gateway kill{no,yes} x 5 reps,
         load 5000 ev/s (50 rooms x 20 clients x 5 ev/s) -> 250 rooms per cell.
"""
import csv, random, sys

REPS = int(sys.argv[1]) if len(sys.argv) > 1 else 5
SEED = 20261101
CLIENTS, RATE = 20, 5.0

def rooms_for(load):  # total events/s = rooms * clients * rate
    return int(load / (CLIENTS * RATE))

rows = []
for broker in ["pubsub", "streams", "kafka", "kafka-tuned"]:
    for rep in range(2):
        rows.append(dict(stage="A", broker=broker, load=200, rooms=rooms_for(200), clients=CLIENTS, rate=RATE,
                         disconnects_per_min=0, gateway_kill=0, rep=rep))
for broker in ["pubsub", "streams", "kafka", "kafka-tuned"]:
    for load in [1000, 2500, 5000]:
        for rep in range(REPS):
            rows.append(dict(stage="B", broker=broker, load=load, rooms=rooms_for(load), clients=CLIENTS, rate=RATE,
                             disconnects_per_min=0, gateway_kill=0, rep=rep))
for broker in ["pubsub", "streams", "kafka-tuned"]:
    for nu in [0.01, 1]:
        for kill in [0, 1]:
            for rep in range(REPS):
                rows.append(dict(stage="C", broker=broker, load=5000, rooms=rooms_for(5000), clients=CLIENTS,
                                 rate=RATE, disconnects_per_min=nu, gateway_kill=kill, rep=rep))

random.Random(SEED).shuffle(rows)  # randomized run order (calibration first)
rows.sort(key=lambda r: r["stage"] != "A")
for i, r in enumerate(rows):
    r["run_id"] = f"{i:03d}-{r['stage']}-{r['broker']}-L{r['load']}-nu{r['disconnects_per_min']}-k{r['gateway_kill']}-r{r['rep']}"
with open("configs/matrix.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader(); w.writerows(rows)
print(f"{len(rows)} runs written to configs/matrix.csv")
