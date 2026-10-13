# Running the experiment on AWS EC2 (on demand)

Nothing in this repository creates AWS resources; all steps below are performed by you.

## 1. Instance
- Recommended: **c7i.4xlarge** (16 vCPU, 32 GiB), region eu-central-1 (Frankfurt), Ubuntu 24.04 LTS, 50 GiB gp3.
  On-demand Linux price on 13.10.2026 (AWS price list, EU Frankfurt): **$0.8148/h**.
  A full matrix (~12 h) therefore costs ≈ **$10** compute (+ a few cents of EBS for the days the volume exists).
- Cheaper alternative: m7i.2xlarge (8 vCPU, 32 GiB, $0.483/h) — then set `SHARDS=2` and
  `CPUSET_GATEWAYS=0-2 CPUSET_REDIS=3 CPUSET_KAFKA=4 CPUSET_LOADGEN=5-7`, and verify in the pilot that the load
  generator stays below ~70 % CPU, otherwise it becomes the bottleneck.
- Security group: SSH (22) from your IP only. No other inbound ports are needed.

## 2. Prepare the host
```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git python3-venv
sudo usermod -aG docker $USER && newgrp docker
git clone https://github.com/<you>/interactive-learning-broker-bench.git && cd interactive-learning-broker-bench
docker compose build
python3 -m venv .venv && .venv/bin/pip install -r analysis/requirements.txt
```

## 3. Pilot (≈15 min) — mandatory
```bash
head -n 9 experiments/configs/matrix.csv > /tmp/pilot.csv     # header + 8 calibration runs
DURATION_S=60 ./experiments/run_matrix.sh /tmp/pilot.csv
docker stats --no-stream                                       # in a second SSH session during the run
```
Check: every `results/<run>/shard-*/summary.json` exists, Pub/Sub/Streams R = 1 without faults, load generator
CPU < 70 %. Then delete the pilot results: `rm -rf results/0*`.

## 4. Full run (≈12 h) inside tmux
```bash
tmux new -s bench
python3 experiments/make_matrix.py
./experiments/run_matrix.sh 2>&1 | tee results/run.log      # resumable: re-run the same command after interruption
```

## 5. Collect results and stop paying
```bash
.venv/bin/python analysis/analyze.py results experiments/configs/matrix.csv
tar czf results.tgz results     # then on your laptop: scp ubuntu@<ip>:~/interactive-learning-broker-bench/results.tgz .
```
**Terminate the instance** (and delete the EBS volume if it was not set to delete on termination).
