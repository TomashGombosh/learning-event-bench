# Shared by every run. bench-down drops this resource from state and does not delete it
# (prevent_destroy). Streams stay readable after the instance is gone. Retention is 30 days.
# bench-up imports the group when it already exists so the next apply does not create it again.
resource "aws_cloudwatch_log_group" "bench" {
  name              = "/leb/bench"
  retention_in_days = 30

  lifecycle {
    prevent_destroy = true
  }
}
