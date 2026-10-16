# Shared by every run that uses this state. bench-down destroys it with the stack;
# a failed setup also copies /var/log/leb-setup.log to s3://<results>/runs/<run_id>/setup-failed.log.
resource "aws_cloudwatch_log_group" "bench" {
  name              = "/leb/bench"
  retention_in_days = 14
}
