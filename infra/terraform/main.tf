data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_s3_bucket" "results" {
  bucket = var.results_bucket
}

resource "time_static" "created" {}

locals {
  ssh_enabled = var.my_ip != "" && var.public_key != ""
}

resource "aws_key_pair" "bench" {
  count      = local.ssh_enabled ? 1 : 0
  key_name   = "leb-${var.run_id}"
  public_key = var.public_key
}

resource "aws_security_group" "bench" {
  name        = "leb-${var.run_id}"
  description = "learning-event-bench: outbound only (+ optional SSH from operator IP)"
  vpc_id      = data.aws_vpc.default.id

  dynamic "ingress" {
    for_each = local.ssh_enabled ? [var.my_ip] : []
    content {
      description = "SSH from operator"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    description = "Outbound (apt, Docker Hub, GitHub, S3)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "bench" {
  ami                                  = data.aws_ami.ubuntu.id
  instance_type                        = var.instance_type
  key_name                             = local.ssh_enabled ? aws_key_pair.bench[0].key_name : null
  vpc_security_group_ids               = [aws_security_group.bench.id]
  iam_instance_profile                 = aws_iam_instance_profile.bench.name
  instance_initiated_shutdown_behavior = "terminate"

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url            = var.repo_url
    git_ref             = var.git_ref
    results_bucket      = var.results_bucket
    run_id              = var.run_id
    bench_reps          = var.bench_reps
    bench_duration_s    = var.bench_duration_s
    max_runtime_minutes = var.max_runtime_minutes
    region              = var.region
  })

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.volume_size_gb
    delete_on_termination = true
    encrypted             = true
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = {
    Name      = "leb-${var.run_id}"
    RunId     = var.run_id
    CreatedAt = time_static.created.rfc3339
  }
}
