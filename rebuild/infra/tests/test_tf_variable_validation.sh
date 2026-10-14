#!/usr/bin/env bash
# Offline test of the Terraform variable validations (no provider, no AWS): copies each root's variables.tf next to a stub that only
# echoes the variables, then runs `terraform plan` with good and bad values. Usage: TF=/path/to/terraform tests/test_tf_variable_validation.sh
set -u
TF="${TF:-terraform}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$HERE/../terraform"
W="$(mktemp -d)"; trap 'rm -rf "$W"' EXIT
fail=0

setup() { # root
  local d="$W/$1"; mkdir -p "$d"
  cp "$ROOT/$1/variables.tf" "$d/"
  # one output per variable declared in the file
  grep -o '^variable "[a-z_]*"' "$ROOT/$1/variables.tf" | sed 's/variable "\(.*\)"/output "\1" { value = var.\1 }/' > "$d/echo.tf"
  # the sensitive-looking outputs are fine: nothing is real
  ( cd "$d" && "$TF" init -backend=false -input=false >/dev/null 2>&1 )
  echo "$d"
}
expect() { # name want(ok|fail) dir args...
  local name="$1" want="$2" d="$3"; shift 3
  if ( cd "$d" && "$TF" plan -input=false -no-color "$@" >"$d/out.txt" 2>&1 ); then got=ok; else got=fail; fi
  if [ "$got" = "$want" ]; then echo "ok   $name"; else echo "FAIL $name (wanted $want, got $got)"; sed -n 1,12p "$d/out.txt"; fail=1; fi
}

D="$(setup cluster)"
ADDONS='addon_versions={"coredns"="v1.12.4-eksbuild.1","kube-proxy"="v1.35.0-eksbuild.2","vpc-cni"="v1.20.4-eksbuild.1","eks-pod-identity-agent"="v1.3.9-eksbuild.3","aws-ebs-csi-driver"="v1.50.1-eksbuild.1"}'
GOOD=(-var account_id=000000000000 -var owner=team -var campaign_id=ec-a -var workload=ecommerce -var state_bucket=b -var 'api_allowed_cidrs=["203.0.113.7/32"]' -var node_ami_release_version=1.35.0-20261003 -var "$ADDONS" -var python_bin=/opt/venv/bin/python)
expect "cluster: good values pass" ok "$D" "${GOOD[@]}"
expect "cluster: 0.0.0.0/0 is rejected" fail "$D" "${GOOD[@]}" -var 'api_allowed_cidrs=["0.0.0.0/0"]'
expect "cluster: a /8 is rejected" fail "$D" "${GOOD[@]}" -var 'api_allowed_cidrs=["10.0.0.0/8"]'
expect "cluster: one good and one open range is rejected" fail "$D" "${GOOD[@]}" -var 'api_allowed_cidrs=["203.0.113.7/32","0.0.0.0/0"]'
expect "cluster: empty list is rejected" fail "$D" "${GOOD[@]}" -var 'api_allowed_cidrs=[]'
expect "cluster: a /16 is allowed" ok "$D" "${GOOD[@]}" -var 'api_allowed_cidrs=["198.51.100.0/24"]'
expect "cluster: 10-character campaign id is rejected" fail "$D" "${GOOD[@]}" -var campaign_id=pilot-ecom
expect "cluster: unknown campaign shape is rejected" fail "$D" "${GOOD[@]}" -var campaign_id=EC_A
expect "cluster: pilot-iot (9 characters) is accepted" ok "$D" "${GOOD[@]}" -var campaign_id=pilot-iot -var workload=iot
expect "cluster: account id must be 12 digits" fail "$D" "${GOOD[@]}" -var account_id=12345
expect "cluster: owner must not be an e-mail" fail "$D" "${GOOD[@]}" -var owner=someone@example.org
expect "cluster: workload must be known" fail "$D" "${GOOD[@]}" -var workload=streaming
expect "cluster: kubernetes 1.30 is rejected" fail "$D" "${GOOD[@]}" -var kubernetes_version=1.30
expect "cluster: AMI release must look like <ver>-<date>" fail "$D" "${GOOD[@]}" -var node_ami_release_version=latest
expect "cluster: add-on map missing a key is rejected" fail "$D" "${GOOD[@]}" -var 'addon_versions={"coredns"="v1.12.4-eksbuild.1"}'
expect "cluster: add-on version must be pinned" fail "$D" "${GOOD[@]}" -var 'addon_versions={"coredns"="latest","kube-proxy"="v1.35.0-eksbuild.2","vpc-cni"="v1.20.4-eksbuild.1","eks-pod-identity-agent"="v1.3.9-eksbuild.3","aws-ebs-csi-driver"="v1.50.1-eksbuild.1"}'
# REVIEW4 M9: the placeholders of addon-versions.auto.tfvars.example are refused by name
expect "cluster: placeholder AMI release is rejected" fail "$D" "${GOOD[@]}" -var node_ami_release_version=1.35.0-20260101
expect "cluster: placeholder add-on version is rejected" fail "$D" "${GOOD[@]}" -var 'addon_versions={"coredns"="v1.0.0-eksbuild.1","kube-proxy"="v1.35.0-eksbuild.2","vpc-cni"="v1.20.4-eksbuild.1","eks-pod-identity-agent"="v1.3.9-eksbuild.3","aws-ebs-csi-driver"="v1.50.1-eksbuild.1"}'
# REVIEW4 B2: the campaign and workload match is a cross-variable validation now
expect "cluster: ec-* with workload iot is rejected" fail "$D" "${GOOD[@]}" -var workload=iot
expect "cluster: pilot-iot with workload ecommerce is rejected" fail "$D" "${GOOD[@]}" -var campaign_id=pilot-iot
expect "cluster: iot-b with workload iot passes" ok "$D" "${GOOD[@]}" -var campaign_id=iot-b -var workload=iot
# REVIEW4 M8: python_bin is required and absolute
expect "cluster: relative python_bin is rejected" fail "$D" "${GOOD[@]}" -var python_bin=python3
# no defaults for the version variables and python_bin: omitting them must fail
MIN=(-var account_id=000000000000 -var owner=team -var campaign_id=ec-a -var workload=ecommerce -var state_bucket=b -var 'api_allowed_cidrs=["203.0.113.7/32"]')
expect "cluster: AMI release and add-on versions have no defaults" fail "$D" "${MIN[@]}" -var python_bin=/opt/venv/bin/python
expect "cluster: python_bin has no default" fail "$D" "${MIN[@]}" -var node_ami_release_version=1.35.0-20261003 -var "$ADDONS"

D="$(setup network)"
NG=(-var account_id=000000000000 -var owner=team -var vpc_cidr=10.123.0.0/16 -var workload_az=us-east-2a -var 'control_plane_azs=["us-east-2b","us-east-2c"]' -var python_bin=/opt/venv/bin/python)
expect "network: good values pass" ok "$D" "${NG[@]}"
expect "network: relative python_bin is rejected" fail "$D" "${NG[@]}" -var python_bin=python3
expect "network: vpc must be a /16" fail "$D" "${NG[@]}" -var vpc_cidr=10.123.0.0/20
expect "network: two distinct control-plane zones" fail "$D" "${NG[@]}" -var 'control_plane_azs=["us-east-2b","us-east-2b"]'
expect "network: one control-plane zone is rejected" fail "$D" "${NG[@]}" -var 'control_plane_azs=["us-east-2b"]'
expect "network: two campaigns on one slot are rejected" fail "$D" "${NG[@]}" -var 'campaign_slots={"ec-a"=1,"ec-b"=1}'

D="$(setup shared)"
SG=(-var account_id=000000000000 -var owner=team -var bucket_suffix=0a1b2c3d)
expect "shared: good values pass" ok "$D" "${SG[@]}"
expect "shared: bucket suffix has no default" fail "$D" -var account_id=000000000000 -var owner=team
expect "shared: bucket suffix must not be an account id" fail "$D" "${SG[@]}" -var bucket_suffix=000000000000
expect "shared: bucket suffix must be lower-case letters or digits" fail "$D" "${SG[@]}" -var bucket_suffix=Ab_1
expect "shared: account id must be 12 digits" fail "$D" "${SG[@]}" -var account_id=abc
expect "shared: negative budget is rejected" fail "$D" "${SG[@]}" -var budget_limit_usd=-1

exit $fail
