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
3. Role, e.g. **`leb-github-actions`**:
   - trust policy: `docs/aws/github-oidc-trust-policy.json` (only `TomashGombosh/learning-event-bench`, branch `main`);
   - inline policy: `docs/aws/github-deploy-policy.json`.
   Replace `<ACCOUNT_ID>`, `<STATE_BUCKET>`, `<RESULTS_BUCKET>` in all three files.

What the deploy role can do: read/write only `bench/*` in the state bucket; read the results bucket; create
EC2 instances (only c7i.4xlarge, m7i.2xlarge, m7i.4xlarge), security groups and key pairs; terminate/delete only
resources tagged `Project=learning-event-bench`; create/delete IAM roles and instance profiles named `leb-instance-*`
**only with the boundary attached**, and pass them to EC2 only. The instance role itself may only `PutObject`/`ListBucket`
under `runs/<run_id>/` of the results bucket (no delete).

## 3. GitHub repository settings
| Kind | Name | Value |
|---|---|---|
| Secret | `AWS_ROLE_ARN` | `arn:aws:iam::<ACCOUNT_ID>:role/leb-github-actions` |
| Variable | `AWS_REGION` | `eu-central-1` |
| Variable | `TF_STATE_BUCKET` | `<prefix>-tfstate` |
| Variable | `RESULTS_BUCKET` | `<prefix>-results` |
| Variable | `PERMISSIONS_BOUNDARY_ARN` | `arn:aws:iam::<ACCOUNT_ID>:policy/leb-instance-boundary` |

The repository must be public (the instance clones it anonymously at the exact commit of the workflow run).

## 4. Run
- **Actions → bench-up → Run workflow** (instance type, repetitions, measurement window).
  Duration ≈ runs × (duration + 90 s); 5 reps × 128 runs ≈ 12 h. Keep it below 40 h.
- Progress: `aws s3 ls s3://<prefix>-results/runs/<run_id>/results/` or the `logs/` folder.
- **bench-down** runs hourly and destroys the stack when the instance is ≥ 41 h old, or finished/terminated early.
  Use *Run workflow* with `force = true` to stop immediately.
- **bench-results → Run workflow** (empty run id = latest) produces an artifact with `_analysis/*.csv`.
  Locally: `RESULTS_BUCKET=<prefix>-results ./experiments/fetch_results.sh [run_id]`.

## Layers of protection against runaway cost
1. The instance self-terminates 5 minutes after the matrix finishes.
2. `shutdown -h +2460` in user_data and `instance_initiated_shutdown_behavior = terminate` (41 h hard stop,
   works even if GitHub is unavailable).
3. bench-down hourly: destroys at ≥ 41 h, or when the instance is no longer running.
4. Deploy policy denies other instance types. Recommended extra: an AWS Budget with an e-mail alert (e.g. $30/month).

## Cost (on-demand Linux, EU Frankfurt, AWS price list 13.10.2026)
c7i.4xlarge $0.8148/h → 12 h ≈ $9.8, 41 h worst case ≈ $33.4. m7i.2xlarge $0.483/h → 41 h ≈ $19.8.
S3 storage for results (tens of MB) and state is negligible; GitHub Actions minutes for hourly checks are about 1 min each.
