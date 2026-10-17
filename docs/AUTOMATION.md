# Automated runs with GitHub Actions + AWS (OIDC)

```
bench-up (dispatch / optional cron) ──terraform apply──▶ EC2 (user_data → systemd leb-bench)
                                                         │  every 10 min: aws s3 sync → s3://<results>/runs/<run_id>/
                                                         │  end: analysis + DONE.json, then self-terminate (+5 min)
                                                         └  safety: shutdown -h +2460 (41 h), shutdown behaviour = terminate
bench-down (hourly cron / dispatch) ── age ≥ 41 h, or instance gone/finished, or force ──▶ terraform destroy
bench-results (dispatch) ── download runs/<run_id>/ from S3 → analysis/analyze.py → workflow artifact
```

Nothing in this repository applies anything to AWS by itself. You perform steps 1–4 once.

## 1. Bootstrap (once, from your laptop, admin credentials)
```bash
cd infra/terraform/bootstrap
terraform init && terraform apply -var name_prefix=leb-tomash   # choose a globally unique prefix
```
Creates `<prefix>-tfstate` (Terraform state, versioned, S3-native locking via `use_lockfile`) and
`<prefix>-results` (versioned, SSE-S3, public access blocked, TLS-only, lifecycle: Glacier IR after 90 days,
non-current versions expire after 30 days). Keep the bootstrap state file locally (or commit nothing of it).

## 2. IAM (once, console or CLI)
1. OIDC identity provider `token.actions.githubusercontent.com`, audience `sts.amazonaws.com`
   (skip if it already exists in the account).
2. Managed policy **`leb-instance-boundary`** from `docs/aws/leb-instance-boundary.json`. It is the permissions
   boundary that caps any instance role the workflow creates (prevents privilege escalation via PassRole).
   If that policy already exists (some accounts named the same document `PutResultsToS3Policy`), edit it in place.
   Terraform does not update it. Add this statement or `PutLogEvents` is denied even after the inline policy allows it:
   ```json
   {
     "Sid": "BenchLogs",
     "Effect": "Allow",
     "Action": ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"],
     "Resource": [
       "arn:aws:logs:*:*:log-group:/leb/bench",
       "arn:aws:logs:*:*:log-group:/leb/bench:*"
     ]
   }
   ```
3. Role, e.g. **`leb-github-actions`**:
   - trust policy: `docs/aws/github-oidc-trust-policy.json` (only `TomashGombosh/learning-event-bench`, branch `main`);
   - inline policy: `docs/aws/github-deploy-policy.json`.
   Replace `<ACCOUNT_ID>`, `<STATE_BUCKET>`, `<RESULTS_BUCKET>` in all three files.
   If the inline policy already exists, add the `BenchLogGroup` statement from that file
   (`logs:CreateLogGroup`, `logs:PutRetentionPolicy`, `logs:DeleteLogGroup`, `logs:DescribeLogGroups`,
   `logs:TagResource`, `logs:ListTagsForResource` on `arn:aws:logs:*:*:log-group:/leb/*` and `...:/leb/*:*`).
   Without it, bench-up cannot create log group `/leb/bench`.

What the deploy role can do: read/write only `bench/*` in the state bucket; read the results bucket; create
EC2 instances (only c7i.large, c7i.xlarge, c7i.2xlarge, c7i.4xlarge, c7g.large, c7g.xlarge, c7g.2xlarge, c7g.4xlarge, m7i.2xlarge, m7i.4xlarge), security groups and key pairs; terminate/delete only
resources tagged `Project=learning-event-bench`; create and delete log group `/leb/bench` (14 day retention);
create/delete IAM roles and instance profiles named `leb-instance-*`
**only with the boundary attached**, and pass them to EC2 only. The instance role itself may only `PutObject`/`ListBucket`
under `runs/<run_id>/` of the results bucket (no delete) and `CreateLogStream`/`PutLogEvents`/`DescribeLogStreams`
on `/leb/bench`.

### Debug reader (separate IAM user)
`docs/aws/debug-reader-policy.json` is read-only, for someone inspecting a run without the deploy role.
Replace `<RESULTS_BUCKET>`, then:

```bash
aws iam create-user --user-name leb-debug-reader
aws iam put-user-policy --user-name leb-debug-reader \
  --policy-name leb-debug-reader \
  --policy-document file://docs/aws/debug-reader-policy.json
```

It allows `logs:GetLogEvents`, `logs:FilterLogEvents`, `logs:DescribeLogStreams`, `logs:DescribeLogGroups`
on `/leb/bench`; `s3:ListBucket`/`s3:GetObject` on `runs/*` of the results bucket; `ec2:DescribeInstances`
and `ec2:GetConsoleOutput`. Streams are `{run_id}/leb-setup.log`, `{run_id}/cloud-init-output.log`,
`{run_id}/bench.log`, and `{run_id}/leb-bench.service`. bench-down deletes the log group with the stack;
a setup failure also uploads `s3://<results>/runs/<run_id>/setup-failed.log`.

## 3. GitHub repository settings
| Kind | Name | Value |
|---|---|---|
| Variable | `AWS_ROLE_ARN` | `arn:aws:iam::<ACCOUNT_ID>:role/leb-github-actions` |
| Variable | `AWS_REGION` | `us-east-1` |
| Variable | `TF_STATE_BUCKET` | `<prefix>-tfstate` |
| Variable | `RESULTS_BUCKET` | `<prefix>-results` |
| Variable | `PERMISSIONS_BOUNDARY_ARN` | `arn:aws:iam::<ACCOUNT_ID>:policy/leb-instance-boundary` |

The repository must be public (the instance clones it anonymously at the exact commit of the workflow run).

## 4. Run
- **Actions → bench-up → Run workflow** (instance type, repetitions, measurement window).
  `bench_reps=1` is a 6-cell smoke plan (not 1 rep of the 128-run factorial). Duration ≈ runs × (duration + 90 s); 5 reps × 128 runs ≈ 12 h. Keep it below 40 h.
  A stuck cell is killed after `(warmup + duration + drain) × 3 + 60 s`. A watchdog of `cells × (compose-up budget + that timeout)` uploads logs to S3 and powers the instance off.
- Progress: `aws s3 ls s3://<prefix>-results/runs/<run_id>/results/` or the `logs/` folder.
  Live setup output is also in CloudWatch log group `/leb/bench`, stream `<run_id>/leb-setup.log`
  (serial console via `aws ec2 get-console-output` if the agent never started).
- **bench-down** runs hourly and destroys the stack when the instance is ≥ 41 h old, or finished/terminated early.
  Use *Run workflow* with `force = true` to stop immediately.
- **bench-results → Run workflow** (empty run id = latest) produces an artifact with `_analysis/*.csv`.
  Locally: `RESULTS_BUCKET=<prefix>-results ./experiments/fetch_results.sh [run_id]`.

## Layers of protection against runaway cost
1. The instance self-terminates 5 minutes after the matrix finishes.
2. Per-cell `timeout` on the load generator, then a matrix-sized watchdog that uploads logs and powers off.
3. `shutdown -h +2460` in user_data and `instance_initiated_shutdown_behavior = terminate` (41 h hard stop,
   works even if GitHub is unavailable).
4. bench-down hourly: destroys at ≥ 41 h, or when the instance is no longer running.
5. Deploy policy denies other instance types. Recommended extra: an AWS Budget with an e-mail alert (e.g. $30/month).

## Cost (on-demand Linux, EU Frankfurt, AWS price list 13.10.2026)
c7g.2xlarge (default, Graviton3, 8 vCPU / 16 GiB) $0.3298/h → 12 h ≈ $4.0, 41 h worst case ≈ $13.5.
Same size in us-east-1 (the `AWS_REGION` example) is $0.2900/h → 12 h ≈ $3.5, 41 h ≈ $11.9.
c7i.4xlarge $0.8148/h → 12 h ≈ $9.8, 41 h worst case ≈ $33.4. m7i.2xlarge $0.483/h → 41 h ≈ $19.8.
S3 storage for results (tens of MB) and state is negligible; GitHub Actions minutes for hourly checks are about 1 min each.
