# Terraform: one EC2 instance for learning-event-bench

Creates in `eu-central-1` (default): an Ubuntu 24.04 instance (`c7i.4xlarge` by default), a key pair from your
public key, a security group allowing **only SSH from `var.my_ip`**, and an encrypted 50 GiB gp3 root volume
(deleted with the instance). Cloud-init installs Docker, Docker Compose v2, tmux and Python venv, and clones
`var.repo_url` into `/home/ubuntu/learning-event-bench`.

Cost (on-demand Linux, EU Frankfurt, AWS price list 13.10.2026): c7i.4xlarge $0.8148/h, m7i.2xlarge $0.483/h.

## Prerequisites
- Terraform >= 1.5, AWS credentials configured (`aws configure` or environment variables), permissions for EC2.
- The repository must be reachable from the instance (public, or use an HTTPS URL with a token).

## Create
```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # fill my_ip, public_key, repo_url
terraform init
terraform plan
terraform apply
terraform output ssh_command
```
Wait 3–5 minutes until `/home/ubuntu/BENCH_READY` exists (`tail -f /var/log/bench-setup.log`), then follow
`docs/EC2.md` from step 3 (pilot).

## Destroy (do not forget: billing is per second while the instance exists)
```bash
# first copy the results to your laptop:
scp ubuntu@$(terraform output -raw public_ip):~/learning-event-bench/results.tgz .
terraform destroy
```
`terraform.tfvars` and `*.tfstate` are git-ignored; keep the state until `destroy` has completed.
