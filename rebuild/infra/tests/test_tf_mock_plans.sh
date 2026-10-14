#!/usr/bin/env bash
# Offline FULL plans of the three Terraform roots with mock providers (`terraform test`, Terraform >= 1.7), plus a dependency
# graph check of the two destroy guards. No AWS call: every provider is mocked and the remote states are overridden.
# Works on a COPY: the tests in tests/tf/<root>/ are copied into <copy>/terraform/<root>/tests/, and a local backend
# override replaces the partial S3 backend so `terraform graph` can run. Nothing under the repository is changed.
#
#   TF=/path/to/terraform-1.10.5 tests/test_tf_mock_plans.sh
# TF_PLUGIN_CACHE_DIR is honoured if set (the cluster providers are about 700 MB).
#
# What it catches (REVIEW4_OPERATIONS): plan-time errors in any root (B1 coalesce on a null profile, B2 "Invalid count
# argument" from a module depends_on), the AMI release pin (C2: no node group may take the release from SSM), the destroy
# order of the teardown guard (B3) and of the network guard (M5), and the plan-level assertions in tests/tf/*.tftest.hcl.
set -u
TF="${TF:-terraform}"
PY="${PYTHON_BIN:-python3}"
HERE="$(cd "$(dirname "$0")" && pwd)"
INFRA="$(cd "$HERE/.." && pwd)"
W="$(mktemp -d)"; trap 'rm -rf "$W"' EXIT
fail=0
ok()  { echo "ok   $*"; }
bad() { echo "FAIL $*"; fail=1; }

rsync -a --exclude .terraform --exclude '*.tfstate*' --exclude '*.tfvars' --exclude backend.hcl "$INFRA/" "$W/infra/"
# The copy gets the lock as it is before scripts/build_images.sh runs: the built images without tag or digest. The run
# empty_digests_stop_the_plan needs that state, and the committed lock has real digests once the images are built (found in
# the pilot, 14 October 2026).
awk '/^[a-z_]+:/ { sect = $1 } sect == "built:" && /^    (tag|digest): / { sub(/: .*/, ": \"\"") } { print }' \
  "$INFRA/images/images.lock.yaml" > "$W/infra/images/images.lock.yaml"
grep -A3 '^built:' "$W/infra/images/images.lock.yaml" | grep -q 'digest: ""\|tag: ""' || { bad "could not blank the built digests in the copy"; exit 1; }
PINNED="1.35.4-20261003"     # tests/tf/cluster/plan.tftest.hcl, var.node_ami_release_version
SENTINEL="1.35.9-29991231"   # the same file, the mocked SSM "recommended" release

for r in shared network cluster; do
  d="$W/infra/terraform/$r"
  mkdir -p "$d/tests"
  cp "$INFRA/tests/tf/$r/plan.tftest.hcl" "$d/tests/"
  printf 'terraform {\n  backend "local" {}\n}\n' > "$d/backend_override.tf"
  if ! (cd "$d" && "$TF" init -input=false -no-color > "$W/init-$r.log" 2>&1); then
    bad "mock plan $r: init"; tail -20 "$W/init-$r.log"; continue
  fi
  if (cd "$d" && "$TF" test -verbose -no-color > "$W/test-$r.log" 2>&1); then
    ok "mock plan $r: $(grep -E '^(Success|Failure)!' "$W/test-$r.log")"
  else
    bad "mock plan $r"; grep -E 'Error|fail|Failure' "$W/test-$r.log" | head -30
  fi
  if grep -q 'Error: Invalid count argument' "$W/test-$r.log"; then bad "mock plan $r: Invalid count argument (module depends_on, B2)"; fi
  if grep -q 'Call to function "coalesce" failed' "$W/test-$r.log"; then bad "mock plan $r: coalesce on a null value (B1)"; fi
  if grep -qE '# .* (will be destroyed|must be replaced|will be updated in-place)' "$W/test-$r.log"; then bad "mock plan $r: a first plan must only create"; fi
done

# C2: every node group takes the pinned release, none the SSM one
if [ -f "$W/test-cluster.log" ]; then
  n_pin="$(grep -c "release_version *= \"$PINNED\"" "$W/test-cluster.log")"
  n_ssm="$(grep -c "$SENTINEL" "$W/test-cluster.log")"
  # first_plan, first_plan_with_profile and first_plan_iot each print three node groups
  if [ "$n_ssm" = 0 ] && [ "$n_pin" -ge 3 ] && [ $((n_pin % 3)) = 0 ]; then ok "node groups use the pinned AMI release ($n_pin node group plans, 0 from SSM)"
  else bad "node groups: pinned release $n_pin times, SSM release $n_ssm times (C2)"; fi
fi

# B3 and M5: each guard is destroyed before everything else in its root
for pair in cluster:terraform_data.teardown_guard network:terraform_data.network_guard; do
  root="${pair%%:*}"; guard="${pair#*:}"
  d="$W/infra/terraform/$root"
  if (cd "$d" && "$TF" graph -no-color > "$W/graph-$root.dot" 2> "$W/graph-$root.err"); then
    "$PY" "$HERE/check_tf_graph.py" "$W/graph-$root.dot" --guard "$guard" || fail=1
  else
    bad "terraform graph $root"; cat "$W/graph-$root.err"
  fi
done

exit $fail
