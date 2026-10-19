variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-central-1"
}

variable "instance_type" {
  description = "EC2 instance type. Default c7g.4xlarge (Graviton3, 16 vCPU / 32 GiB, us-east-1). The Ubuntu AMI matches supported_architectures (arm64 or x86_64)."
  type        = string
  default     = "c7g.4xlarge"
}

variable "results_bucket" {
  description = "Name of the results bucket created by the bootstrap stack."
  type        = string
}

variable "run_id" {
  description = "Identifier of this benchmark run; results go to s3://<results_bucket>/runs/<run_id>/."
  type        = string
}

variable "repo_url" {
  description = "Git URL of the repository to clone on the instance (must be publicly readable)."
  type        = string
  default     = "https://github.com/TomashGombosh/learning-event-bench.git"
}

variable "git_ref" {
  description = "Commit SHA or branch to check out on the instance."
  type        = string
  default     = "main"
}

variable "bench_profile" {
  description = "smoke = 17 cells x 60s (default, ~45-60 min). kafka-rerun = ~40 Kafka cells. full = factorial using bench_reps and bench_duration_s."
  type        = string
  default     = "smoke"

  validation {
    condition     = contains(["smoke", "kafka-rerun", "full"], var.bench_profile)
    error_message = "bench_profile must be smoke, kafka-rerun, or full."
  }
}

variable "bench_reps" {
  description = "Repetitions per matrix cell when bench_profile=full (5 -> 128 runs, ~12.5 h on c7g.4xlarge). Ignored for smoke."
  type        = number
  default     = 5
}

variable "bench_duration_s" {
  description = "Measurement window per run, seconds, when bench_profile is full or kafka-rerun. Smoke forces 60s."
  type        = number
  default     = 240
}

variable "max_runtime_minutes" {
  description = "Safety timeout: the instance powers off (and terminates) after this many minutes."
  type        = number
  default     = 2460 # 41 h
}

variable "volume_size_gb" {
  description = "Root EBS gp3 volume size in GiB."
  type        = number
  default     = 50
}

variable "my_ip" {
  description = "Optional: your IPv4 CIDR for SSH debugging (e.g. 203.0.113.10/32). Empty = no inbound access."
  type        = string
  default     = ""
}

variable "public_key" {
  description = "Optional: SSH public key for debugging. Empty = no key pair."
  type        = string
  default     = ""
}

variable "permissions_boundary_arn" {
  description = "Optional IAM permissions boundary for the instance role (recommended; required by the deploy policy in docs/aws)."
  type        = string
  default     = null
}
