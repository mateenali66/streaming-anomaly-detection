output "vpc_id" {
  value = aws_vpc.this.id
}

output "vpc_cidr" {
  value = aws_vpc.this.cidr_block
}

output "workload_az" {
  value = var.workload_az
}

output "slot_subnet_by_campaign" {
  value       = { for k, v in var.campaign_slots : k => aws_subnet.slot[v].id }
  description = "Campaign id => node subnet. The cluster root reads this, so no subnet id is copied by hand."
}

output "control_plane_subnet_ids" {
  value = aws_subnet.cp[*].id
}

output "nat_note" {
  value = "One NAT gateway in ${var.workload_az}: about $0.045 per hour plus $0.045 per GB."
}
