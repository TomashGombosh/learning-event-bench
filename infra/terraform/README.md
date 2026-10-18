# Terraform for learning-event-bench

- `bootstrap/` — long-lived S3 buckets for Terraform state (S3-native locking) and results. Apply once locally.
- `.` (this folder) — one benchmark run: EC2 instance (default c7g.4xlarge, Ubuntu 24.04 amd64 or arm64 matching the instance type), security group (outbound only;
  optional SSH from `my_ip`), encrypted 50 GiB gp3 volume, IAM instance profile that may only write to
  `s3://<results>/runs/<run_id>/` and publish logs to `/leb/bench`. `user_data` clones the repo at `git_ref`, starts the matrix under systemd, syncs results
  to S3 every 10 min, writes `DONE.json`, then self-terminates. Setup and bench logs go to CloudWatch `/leb/bench`
  (kept across destroys, 30 day retention). Safety timeout: `shutdown -h +2460` with
  `instance_initiated_shutdown_behavior = terminate`.

Normally applied/destroyed by GitHub Actions (`bench-up`, `bench-down`), see `docs/AUTOMATION.md`.

## Manual use
```bash
terraform init -backend-config="bucket=<prefix>-tfstate" -backend-config="key=bench/terraform.tfstate" -backend-config="region=eu-central-1"
cp terraform.tfvars.example terraform.tfvars   # results_bucket, run_id, optional SSH
terraform apply
terraform state rm aws_cloudwatch_log_group.bench || true   # keep /leb/bench
terraform destroy                               # always, after the results are in S3
```
Requires Terraform >= 1.10 (S3 native state locking).
