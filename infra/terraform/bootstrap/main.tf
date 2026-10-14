# One-time, long-lived resources (apply locally once, never destroyed by the workflows):
#  - S3 bucket for Terraform remote state (S3 native locking, use_lockfile = true; Terraform >= 1.10)
#  - S3 bucket for benchmark results
terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = "learning-event-bench" }
  }
}

variable "region" {
  type    = string
  default = "eu-central-1"
}

variable "name_prefix" {
  description = "Globally unique prefix for bucket names, e.g. leb-tomash."
  type        = string
}

variable "results_noncurrent_days" {
  description = "Days to keep non-current (overwritten) versions of result objects."
  type        = number
  default     = 30
}

variable "results_glacier_days" {
  description = "Days after which result objects move to Glacier Instant Retrieval."
  type        = number
  default     = 90
}

locals {
  buckets = {
    state   = "${var.name_prefix}-tfstate"
    results = "${var.name_prefix}-results"
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets
  bucket   = each.value
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.this]
}

resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.this["state"].id
  rule {
    id     = "expire-old-state-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.this["results"].id
  rule {
    id     = "results-retention"
    status = "Enabled"
    filter {
      prefix = "runs/"
    }
    transition {
      days          = var.results_glacier_days
      storage_class = "GLACIER_IR"
    }
    noncurrent_version_expiration {
      noncurrent_days = var.results_noncurrent_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

output "state_bucket" {
  value = aws_s3_bucket.this["state"].id
}

output "results_bucket" {
  value = aws_s3_bucket.this["results"].id
}
