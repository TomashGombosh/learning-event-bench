output "instance_id" {
  value = aws_instance.bench.id
}

output "public_ip" {
  value = aws_instance.bench.public_ip
}

output "created_at" {
  description = "RFC3339 creation time (also the CreatedAt tag); used by bench-down to enforce the 41 h limit."
  value       = time_static.created.rfc3339
}

output "results_prefix" {
  value = "s3://${var.results_bucket}/runs/${var.run_id}/"
}

output "ssh_command" {
  value = local.ssh_enabled ? "ssh ubuntu@${aws_instance.bench.public_ip}" : "SSH disabled (set my_ip and public_key to enable)"
}

output "run_id" {
  value = var.run_id
}
