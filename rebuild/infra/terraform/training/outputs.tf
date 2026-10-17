output "instance_id" {
  value = aws_instance.training.id
}

output "ami_id" {
  value       = aws_instance.training.ami
  description = "The Deep Learning Base AMI the instance runs. Recorded in the log for the compute statement."
  sensitive   = false
}

output "instance_type" {
  value = aws_instance.training.instance_type
}

output "cap_hours" {
  value = var.cap_hours
}

output "training_prefix" {
  value = "training/"
}
