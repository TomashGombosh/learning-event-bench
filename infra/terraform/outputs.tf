output "public_ip" {
  description = "Public IP of the bench instance."
  value       = aws_instance.bench.public_ip
}

output "ssh_command" {
  description = "SSH command (use the private key matching var.public_key)."
  value       = "ssh ubuntu@${aws_instance.bench.public_ip}"
}

output "instance_type" {
  value = aws_instance.bench.instance_type
}
