# Instance role: may only write (put/list) under runs/ of the results bucket. No delete, no other services.
data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bench" {
  name                 = "leb-instance-${var.run_id}"
  assume_role_policy   = data.aws_iam_policy_document.assume_ec2.json
  permissions_boundary = var.permissions_boundary_arn == "" ? null : var.permissions_boundary_arn
}

data "aws_iam_policy_document" "results_write" {
  statement {
    sid       = "WriteResults"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${data.aws_s3_bucket.results.arn}/runs/${var.run_id}/*"]
  }
  statement {
    sid       = "ListOwnPrefix"
    actions   = ["s3:ListBucket"]
    resources = [data.aws_s3_bucket.results.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["runs/${var.run_id}/*", "runs/${var.run_id}"]
    }
  }
}

resource "aws_iam_role_policy" "results_write" {
  name   = "results-write"
  role   = aws_iam_role.bench.id
  policy = data.aws_iam_policy_document.results_write.json
}

resource "aws_iam_instance_profile" "bench" {
  name = "leb-instance-${var.run_id}"
  role = aws_iam_role.bench.name
}
