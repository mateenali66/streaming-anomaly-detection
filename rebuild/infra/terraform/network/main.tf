provider "aws" {
  region              = var.region
  profile             = var.aws_profile
  allowed_account_ids = [var.account_id]

  default_tags {
    tags = {
      Project   = var.project
      Scope     = "network"
      ManagedBy = "terraform"
      Owner     = var.owner
    }
  }
}

locals {
  n = "${var.project}-net"
}

check "slots_cover_campaigns" {
  assert {
    condition     = alltrue([for k, v in var.campaign_slots : v < var.slots])
    error_message = "A campaign slot index is not below var.slots."
  }
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = { Name = local.n }

  lifecycle {
    # A variable validation may only refer to its own variable before Terraform 1.9, so this cross-variable rule lives here.
    precondition {
      condition     = !contains(var.control_plane_azs, var.workload_az)
      error_message = "control_plane_azs must not contain workload_az: the control-plane subnets are for two OTHER zones."
    }
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = local.n }
}

# --- public /28 for the single NAT, in the SAME zone as the nodes (no cross-zone NAT charge) ---
resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.this.id
  availability_zone = var.workload_az
  cidr_block        = cidrsubnet(var.vpc_cidr, 12, 4032) # x.252.0/28
  tags              = { Name = "${local.n}-public-nat" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = { Name = "${local.n}-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_eip" "nat" {
  domain     = "vpc"
  tags       = { Name = "${local.n}-nat" }
  depends_on = [aws_internet_gateway.this]
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id
  tags          = { Name = local.n }
  depends_on    = [aws_internet_gateway.this]
}

# --- one private route table shared by all slots ---
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this.id
  }
  tags = { Name = "${local.n}-private" }
}

# --- one /22 (1019 usable addresses; the VPC CNI gives every pod a VPC address) per cluster slot ---
resource "aws_subnet" "slot" {
  count             = var.slots
  vpc_id            = aws_vpc.this.id
  availability_zone = var.workload_az
  cidr_block        = cidrsubnet(var.vpc_cidr, 6, count.index) # x.0.0/22, x.4.0/22, ...
  tags = {
    Name                              = "${local.n}-slot-${count.index}"
    "${var.project}:slot"             = tostring(count.index)
    "kubernetes.io/role/internal-elb" = "1"
  }
}

resource "aws_route_table_association" "slot" {
  count          = var.slots
  subnet_id      = aws_subnet.slot[count.index].id
  route_table_id = aws_route_table.private.id
}

# --- control-plane-only subnets in two other zones (EKS places a few network interfaces there) ---
resource "aws_subnet" "cp" {
  count             = 2
  vpc_id            = aws_vpc.this.id
  availability_zone = var.control_plane_azs[count.index]
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 250 + count.index) # x.250.0/24, x.251.0/24
  tags              = { Name = "${local.n}-cp-${count.index}" }
}

resource "aws_route_table_association" "cp" {
  count          = 2
  subnet_id      = aws_subnet.cp[count.index].id
  route_table_id = aws_route_table.private.id
}

# --- S3 gateway endpoint: free; keeps ECR layer pulls and result uploads off the NAT ---
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
  tags              = { Name = "${local.n}-s3" }
}

# --- optional interface endpoints, single zone ---
resource "aws_security_group" "endpoints" {
  count       = var.interface_endpoints ? 1 : 0
  name        = "${local.n}-endpoints"
  description = "HTTPS from the VPC to interface endpoints"
  vpc_id      = aws_vpc.this.id
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
}

resource "aws_vpc_endpoint" "iface" {
  for_each            = var.interface_endpoints ? toset(["ecr.api", "ecr.dkr", "sts"]) : toset([])
  vpc_id              = aws_vpc.this.id
  service_name        = "com.amazonaws.${var.region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.slot[0].id]
  security_group_ids  = [aws_security_group.endpoints[0].id]
  private_dns_enabled = true
  tags                = { Name = "${local.n}-${each.key}" }
}

# --- destroy guard (REVIEW4 M5) ---
# Created LAST, so `terraform destroy` runs it FIRST, before any route table association, subnet or the NAT gateway goes.
# scripts/network_guard.py (boto3, read only) refuses while any EKS cluster named for this project still uses the VPC, and
# lists network interfaces and VPC endpoints that would make the subnet and VPC deletes hang (VPC CNI leftovers carry no
# Campaign tag; a GuardDuty Runtime Monitoring endpoint carries GuardDutyManaged). A refusal stops the destroy with nothing
# removed. To discard the network on purpose while a cluster runs, destroy that cluster first.
resource "terraform_data" "network_guard" {
  input = {
    vpc_id  = aws_vpc.this.id
    region  = var.region
    project = var.project
    script  = abspath("${path.module}/../../scripts/network_guard.py")
    python  = var.python_bin
    profile = var.aws_profile == null ? "" : var.aws_profile
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = "[ -z \"$GUARD_PROFILE\" ] || export AWS_PROFILE=\"$GUARD_PROFILE\"; \"$GUARD_PYTHON\" \"$GUARD_SCRIPT\" --vpc-id \"$GUARD_VPC\" --region \"$GUARD_REGION\" --project \"$GUARD_PROJECT\""
    environment = {
      GUARD_PYTHON  = self.input.python
      GUARD_SCRIPT  = self.input.script
      GUARD_VPC     = self.input.vpc_id
      GUARD_REGION  = self.input.region
      GUARD_PROJECT = self.input.project
      GUARD_PROFILE = self.input.profile # empty means: keep the AWS_PROFILE already exported
    }
  }

  # Everything in this root must be destroyed after the guard. tests/check_tf_graph.py checks reachability.
  depends_on = [
    aws_route_table_association.public,
    aws_route_table_association.slot,
    aws_route_table_association.cp,
    aws_nat_gateway.this,
    aws_vpc_endpoint.s3,
    aws_vpc_endpoint.iface,
  ]
}
