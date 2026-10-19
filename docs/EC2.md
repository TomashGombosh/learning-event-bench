# Running the experiment on AWS EC2 (on demand)

Nothing is created in AWS automatically; all steps below are performed by you.
Recommended: fully automated runs via GitHub Actions, see [AUTOMATION.md](AUTOMATION.md).
Alternatives: **A) Terraform by hand** (`infra/terraform`, now with remote state, see its README) or **B) manual** console setup (sections 1–2).

## 0. Option A: Terraform
```bash
cd infra/terraform && cp terraform.tfvars.example terraform.tfvars   # results_bucket, run_id, my_ip, public_key
terraform init -backend-config="bucket=<prefix>-tfstate" -backend-config="key=bench/terraform.tfstate" -backend-config="region=eu-central-1"
terraform apply
ssh ubuntu@$(terraform output -raw public_ip)
test -f ~/BENCH_READY && cd ~/learning-event-bench && docker compose build
```
Note: with this Terraform the instance starts the full matrix automatically (systemd `leb-bench`) and uploads to S3;
sections 3–4 are only needed for manual runs (`sudo systemctl stop leb-bench` first).
After the run: copy results, then `terraform destroy` (section 5).

## 1. Option B: manual instance
- Automated `bench-up` defaults to **c7g.4xlarge** (Graviton3, 16 vCPU, 32 GiB, us-east-1) and **profile=smoke**. Terraform picks the Ubuntu 24.04 AMI for the instance architecture (`arm64` or `x86_64`). On 16 vCPU the cores are pinned: `gateway1=0 gateway2=1 gateway3=2 redis=3-4 kafka=5-8 loadgen=9-15` (`LOADGEN_CPUS=7`, `SHARDS=7`, one Node process per loadgen core). Kafka gets four cores and a 2G heap (`num.network.threads=8`, `num.io.threads=16`). The generator and the brokers do not share a core. A second loadgen instance is not used: cross-host RTT would change the latency under test, and seven loadgen cores cover L5000 once each shard has its own core.
- 8 vCPU fallback (c7g.2xlarge or m7i.2xlarge): `gateway1=0 gateway2=1 gateway3=2 redis=3 kafka=4 loadgen=5-7` (`SHARDS=3`). m7i.2xlarge is 32 GiB at $0.483/h (EU Frankfurt, 13.10.2026). Before a full profile, the instance runs a 30 s L5000 calibration and powers off if achieved send rate is under 95% of offered.
- On-demand Linux price (AWS price list, 13.10.2026): c7g.4xlarge in us-east-1 is **$0.5800/h** (twice c7g.2xlarge at $0.2900/h). A full matrix (~23 h, with plain kafka and scenario C at L2500 and L5000) therefore costs ≈ **$13.30** compute; the 41 h cap is ≈ **$24**. EU Frankfurt c7g.4xlarge is about $0.6596/h (twice the $0.3298/h 2xlarge). c7i.4xlarge remains $0.8148/h. EBS for the days the volume exists is a few cents. Yjs digest merges updates in chunks of 128 (50k updates: 288 ms vs 485 ms applied one by one on this toolchain).
- Security group: SSH (22) from your IP only. No other inbound ports are needed.

## 2. Prepare the host
```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git python3-venv
sudo usermod -aG docker $USER && newgrp docker
git clone https://github.com/<you>/learning-event-bench.git && cd learning-event-bench
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

## 4. Full run (≈23 h on c7g.4xlarge) inside tmux
```bash
tmux new -s bench
python3 experiments/make_matrix.py
./experiments/run_matrix.sh 2>&1 | tee results/run.log      # resumable: re-run the same command after interruption
```

## 5. Collect results and stop paying
```bash
.venv/bin/python analysis/analyze.py results experiments/configs/matrix.csv
tar czf results.tgz results     # then on your laptop: scp ubuntu@<ip>:~/learning-event-bench/results.tgz .
```
**Stop paying:** Terraform: `cd infra/terraform && terraform destroy`. Manual: terminate the instance (and delete the EBS volume if it was not set to delete on termination).
