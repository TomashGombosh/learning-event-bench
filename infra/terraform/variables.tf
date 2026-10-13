variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-central-1"
}

variable "instance_type" {
  description = "EC2 instance type (c7i.4xlarge: 16 vCPU / 32 GiB; budget option m7i.2xlarge)."
  type        = string
  default     = "c7i.4xlarge"
}

variable "my_ip" {
  description = "Your public IPv4 address in CIDR form, e.g. 203.0.113.10/32. Only this address may SSH in."
  type        = string
  validation {
    condition     = can(cidrhost(var.my_ip, 0))
    error_message = "my_ip must be a CIDR block such as 203.0.113.10/32."
  }
}

variable "key_name" {
  description = "Name of the EC2 key pair to create."
  type        = string
  default     = "learning-event-bench"
}

variable "public_key" {
  description = "SSH public key material (contents of e.g. ~/.ssh/id_ed25519.pub)."
  type        = string
}

variable "repo_url" {
  description = "Git URL of the learning-event-bench repository to clone on the instance."
  type        = string
  default     = "https://github.com/<your-account>/learning-event-bench.git"
}

variable "volume_size_gb" {
  description = "Root EBS gp3 volume size in GiB."
  type        = number
  default     = 50
}
