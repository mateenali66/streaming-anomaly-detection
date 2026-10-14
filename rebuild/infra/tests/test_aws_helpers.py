#!/usr/bin/env python3
"""Offline tests of the boto3 helpers in scripts/ with botocore Stubber and fake clients. No AWS call is made: credentials are fake
and no client is ever pointed at the network. Run with a Python that has boto3 (scripts/requirements.txt)."""
import base64
import datetime as dt
import json
import os
import sys
import urllib.parse
from pathlib import Path

os.environ.update(AWS_ACCESS_KEY_ID="AKIAFAKEFAKEFAKEFAKE", AWS_SECRET_ACCESS_KEY="fake", AWS_DEFAULT_REGION="us-east-2")
os.environ.pop("AWS_PROFILE", None)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import boto3  # noqa: E402
from botocore.stub import ANY, Stubber  # noqa: E402

import capture_versions as cv  # noqa: E402
import delete_orphan_volumes as dov  # noqa: E402
import discover  # noqa: E402
import eks_api_cidrs  # noqa: E402
import eks_kubeconfig as kc  # noqa: E402
import network_guard  # noqa: E402
import eks_token  # noqa: E402
import sweep  # noqa: E402
import teardown_guard as tg  # noqa: E402

failures = 0


def check(name, cond, extra=""):
    global failures
    print(("ok   " if cond else "FAIL ") + name + ("" if cond else f"  {extra}"))
    failures += 0 if cond else 1


# ------------------------------------------------------------------ capture_versions
sess = boto3.Session(region_name="us-east-2")
eks, ssm = sess.client("eks"), sess.client("ssm")


def addon_resp(name, versions):
    return {"addons": [{"addonName": name, "type": "networking", "addonVersions": [
        {"addonVersion": v, "architecture": ["arm64"], "compatibilities": [{"clusterVersion": "1.35", "platformVersions": ["*"], "defaultVersion": d}]}
        for v, d in versions]}]}


with Stubber(eks) as st:
    for n in cv.ADDONS:
        st.add_response("describe_addon_versions", addon_resp(n, [("v9.9.9-eksbuild.2", False), ("v9.9.8-eksbuild.1", True)]), {"kubernetesVersion": "1.35", "addonName": n})
    got = {n: cv.pick_version(eks, n, "1.35", "default") for n in cv.ADDONS}
check("default version is the one AWS marks default", set(got.values()) == {"v9.9.8-eksbuild.1"}, got)
with Stubber(eks) as st:
    st.add_response("describe_addon_versions", addon_resp("coredns", [("v9.9.9-eksbuild.2", False), ("v9.9.8-eksbuild.1", True)]), {"kubernetesVersion": "1.35", "addonName": "coredns"})
    check("latest picks the first listed", cv.pick_version(eks, "coredns", "1.35", "latest") == "v9.9.9-eksbuild.2")
with Stubber(eks) as st:
    st.add_response("describe_addon_versions", addon_resp("coredns", [("v1-eksbuild.1", False)]), {"kubernetesVersion": "1.35", "addonName": "coredns"})
    try:
        cv.pick_version(eks, "coredns", "1.35", "default"); check("no default raises", False)
    except SystemExit:
        check("no default raises", True)
with Stubber(ssm) as st:
    st.add_response("get_parameter", {"Parameter": {"Name": "x", "Value": "1.35.2-20261003", "Type": "String"}},
                    {"Name": "/aws/service/eks/optimized-ami/1.35/amazon-linux-2023/arm64/standard/recommended/release_version"})
    check("ami release read", cv.ami_release(ssm, "1.35") == "1.35.2-20261003")
text = cv.render("1.35", "us-east-2", "1.35.2-20261003", got, dt.datetime(2026, 10, 13, tzinfo=dt.timezone.utc))
check("render has ami and every add-on", 'node_ami_release_version = "1.35.2-20261003"' in text and all(f'"{n}"' in text for n in cv.ADDONS), text)
check("render has no account id, profile or e-mail", not any(s in text for s in ("@", "arn:", "profile")))
schema = {"properties": {"controller": {"properties": {"extraVolumeTags": {}, "tolerations": {}, "nodeSelector": {}}}}}
check("schema path found", cv.has_path(schema, ("controller", "extraVolumeTags")) and not cv.has_path(schema, ("controller", "nope")))
# The CoreDNS schema (v1.13.2-eksbuild.50, read 14 October 2026) keeps its keys behind a root $ref, and nests refs below it.
ref_schema = {"$ref": "#/definitions/Coredns", "$schema": "http://json-schema.org/draft-06/schema#",
              "definitions": {"Coredns": {"additionalProperties": False, "properties": {
                  "nodeSelector": {"type": "object"}, "tolerations": {"items": {}, "type": "array"},
                  "autoScaling": {"$ref": "#/definitions/AutoScaling"}}},
                  "AutoScaling": {"properties": {"enabled": {"type": "boolean"}}}}}
check("schema path found behind a root $ref", cv.has_path(ref_schema, ("tolerations",)) and cv.has_path(ref_schema, ("nodeSelector",))
      and not cv.has_path(ref_schema, ("nope",)))
check("schema path found through a nested $ref", cv.has_path(ref_schema, ("autoScaling", "enabled"))
      and not cv.has_path(ref_schema, ("autoScaling", "nope")))
with Stubber(eks) as st:
    st.add_response("describe_addon_configuration", {"addonName": "aws-ebs-csi-driver", "addonVersion": "v1", "configurationSchema": json.dumps(schema)}, {"addonName": "aws-ebs-csi-driver", "addonVersion": "v1"})
    check("config check passes when keys exist", cv.check_config(eks, "aws-ebs-csi-driver", "v1") == [])
with Stubber(eks) as st:
    st.add_response("describe_addon_configuration", {"addonName": "aws-ebs-csi-driver", "addonVersion": "v1", "configurationSchema": json.dumps({"properties": {"controller": {"properties": {}}}})}, {"addonName": "aws-ebs-csi-driver", "addonVersion": "v1"})
    check("config check names missing keys", "controller.extraVolumeTags" in cv.check_config(eks, "aws-ebs-csi-driver", "v1"))

# ------------------------------------------------------------------ eks_token (signing is local)
tok = eks_token.make_token("streaming-anomaly-detection-ec-a", "us-east-2")
check("token prefix", tok.startswith("k8s-aws-v1."))
raw = tok[len("k8s-aws-v1."):]
url = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)).decode()
q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
check("presigned STS GetCallerIdentity", url.startswith("https://sts.us-east-2.amazonaws.com/") and q["Action"] == ["GetCallerIdentity"], url)
check("cluster id header is signed", "x-k8s-aws-id" in q["X-Amz-SignedHeaders"][0], q.get("X-Amz-SignedHeaders"))
check("signature present, 60 s expiry", "X-Amz-Signature" in q and q["X-Amz-Expires"] == ["60"])
cred = eks_token.exec_credential(tok, dt.datetime(2026, 10, 13, 12, 0, 0, tzinfo=dt.timezone.utc))
check("exec credential shape", cred["kind"] == "ExecCredential" and cred["status"]["expirationTimestamp"] == "2026-10-13T12:14:00Z")

# ------------------------------------------------------------------ eks_kubeconfig
cluster = {"name": "streaming-anomaly-detection-ec-a", "endpoint": "https://example.eks", "certificateAuthority": {"data": "Q0E="},
           "tags": {"Project": "streaming-anomaly-detection", "Campaign": "ec-a"}}
cfg = kc.build_kubeconfig(cluster, "us-east-2", Path("/x/eks_token.py"), "/usr/bin/python3", None)
check("kubeconfig uses the boto3 token script", cfg["users"][0]["user"]["exec"]["command"] == "/usr/bin/python3" and "/x/eks_token.py" in cfg["users"][0]["user"]["exec"]["args"])
kc.check_tags(cluster, "streaming-anomaly-detection", "ec-a")
for bad in ({"tags": {}}, {"tags": {"Project": "other", "Campaign": "ec-a"}}, {"tags": {"Project": "streaming-anomaly-detection", "Campaign": "ec-b"}}):
    try:
        kc.check_tags({**cluster, **bad}, "streaming-anomaly-detection", "ec-a"); check(f"refuses {bad}", False)
    except SystemExit:
        check(f"refuses {bad['tags']}", True)

# ------------------------------------------------------------------ teardown_guard.check_complete
s3 = sess.client("s3")
T0 = dt.datetime(2026, 10, 20, 12, 0, tzinfo=dt.timezone.utc)


def listing(items):
    return {"IsTruncated": False, "Contents": [{"Key": k, "LastModified": t, "Size": 1} for k, t in items], "KeyCount": len(items)}


def run_complete(items, body=b'{"campaign_id": "ec-a", "status": "COMPLETE"}', campaign="ec-a"):
    with Stubber(s3) as st:
        st.add_response("list_objects_v2", listing(items), {"Bucket": "b", "Prefix": f"campaign/{campaign}/"})
        if any(k.endswith("COMPLETE") for k, _ in items):
            import io
            from botocore.response import StreamingBody
            st.add_response("get_object", {"Body": StreamingBody(io.BytesIO(body), len(body))}, {"Bucket": "b", "Key": f"campaign/{campaign}/COMPLETE"})
        return tg.check_complete(s3, "b", campaign)


ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0), ("campaign/ec-a/COMPLETE", T0 + dt.timedelta(minutes=5))])
check("complete: marker newer passes", ok, msg)
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0)])
check("complete: no marker refuses", not ok and "does not exist" in msg, msg)
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0 + dt.timedelta(minutes=9)), ("campaign/ec-a/COMPLETE", T0)])
check("complete: older marker refuses", not ok and "not newer" in msg, msg)
ok, msg = run_complete([("campaign/ec-a/COMPLETE", T0)], body=b'{"campaign_id": "ec-a", "status": "COMPLETE"}')
check("complete: orchestrator marker with no results refuses", not ok, msg)
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0), ("campaign/ec-a/COMPLETE", T0 + dt.timedelta(minutes=5))], body=b'{"campaign_id": "ec-b", "status": "COMPLETE"}')
check("complete: marker for another campaign refuses", not ok and "names campaign" in msg, msg)
# REVIEW4 M4 item 3: the marker body is checked, as complete.py says
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0), ("campaign/ec-a/COMPLETE", T0 + dt.timedelta(minutes=5))], body=b"not json")
check("complete: non-JSON marker refuses", not ok and "not JSON" in msg, msg)
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0), ("campaign/ec-a/COMPLETE", T0 + dt.timedelta(minutes=5))], body=b'{"campaign_id": "ec-a", "status": "RUNNING"}')
check("complete: a status other than COMPLETE or ABANDONED refuses", not ok and "status" in msg, msg)
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0), ("campaign/ec-a/COMPLETE", T0 + dt.timedelta(minutes=5))], body=b'{"campaign_id": "ec-a"}')
check("complete: a marker without status refuses", not ok, msg)
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0), ("campaign/ec-a/COMPLETE", T0 + dt.timedelta(minutes=5))], body=b'{"campaign_id": "ec-a", "status": "ABANDONED"}')
check("complete: orchestrator ABANDONED (abandon --confirm) passes", ok, msg)
MANUAL = b'{"campaign_id": "ec-a", "kind": "manual-marker", "status": "ABANDONED", "author_confirmed": true}'
ok, msg = run_complete([("campaign/ec-a/COMPLETE", T0)], body=MANUAL)
check("complete: a manual marker on an EMPTY prefix passes (M4 item 2)", ok and "empty prefix" in msg, msg)
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0), ("campaign/ec-a/COMPLETE", T0 + dt.timedelta(minutes=5))], body=MANUAL)
check("complete: a manual marker newer than the results passes", ok, msg)
ok, msg = run_complete([("campaign/ec-a/runs/1.parquet", T0 + dt.timedelta(minutes=9)), ("campaign/ec-a/COMPLETE", T0)], body=MANUAL)
check("complete: a manual marker overtaken by a manifest upload refuses and says why (M4 item 4)", not ok and "--phase stop" in msg, msg)
ok, msg = run_complete([("campaign/ec-a/COMPLETE", T0)], body=b'{"campaign_id": "ec-a", "kind": "manual-marker", "status": "ABANDONED"}')
check("complete: a manual marker without author_confirmed refuses", not ok and "author_confirmed" in msg, msg)
src_mc = (Path(tg.__file__).parent / "mark_complete.py").read_text()
check("mark_complete writes the accepted manual form and no user name", '"author_confirmed": True' in src_mc and '"status": "ABANDONED"' in src_mc and "getpass" not in src_mc)
import mark_complete as mc  # noqa: E402
s3 = sess.client("s3")
with Stubber(s3) as st:
    st.add_response("head_object", {"ContentLength": 900}, {"Bucket": "b", "Key": "campaign/pilot-ec/COMPLETE"})
    rc = mc.write_marker(s3, "b", "pilot-ec", "pilot teardown")
    st.assert_no_pending_responses()
check("mark_complete refuses when COMPLETE exists and writes nothing (pilot, 14 October 2026)", rc == 3)
with Stubber(s3) as st:
    st.add_client_error("head_object", service_error_code="404", http_status_code=404,
                        expected_params={"Bucket": "b", "Key": "campaign/pilot-ec/COMPLETE"})
    st.add_response("put_object", {}, {"Bucket": "b", "Key": "campaign/pilot-ec/COMPLETE",
                                       "Body": ANY, "ContentType": "application/json"})
    rc = mc.write_marker(s3, "b", "pilot-ec", "pilot teardown")
    st.assert_no_pending_responses()
check("mark_complete writes the marker when there is none", rc == 0)

# ------------------------------------------------------------------ sweep
class FakePaginator:
    def __init__(self, pages): self.pages = pages
    def paginate(self, **kw): return iter(self.pages)


class FakeClient:
    def __init__(self, data, direct=None): self.data, self.direct = data, direct or {}
    def get_paginator(self, op): return FakePaginator(self.data.get(op, [{}]))
    def __getattr__(self, name):
        if name in self.direct:
            return lambda **kw: self.direct[name]
        raise AttributeError(name)


def tags(**kw): return [{"Key": k, "Value": v} for k, v in kw.items()]


P = "streaming-anomaly-detection"


class RecordingPaginator(FakePaginator):
    def __init__(self, pages, log, op):
        super().__init__(pages); self.log, self.op = log, op
    def paginate(self, **kw):
        self.log.append((self.op, kw)); return iter(self.pages)


class RecordingClient(FakeClient):
    def __init__(self, data, direct=None):
        super().__init__(data, direct); self.calls = []
    def get_paginator(self, op): return RecordingPaginator(self.data.get(op, [{}]), self.calls, op)


ec2 = RecordingClient({
    "describe_volumes": [{"Volumes": [
        {"VolumeId": "vol-1", "Size": 300, "State": "available", "Tags": tags(Campaign="ec-a", Scope="pv", Project=P)},
        {"VolumeId": "vol-2", "Size": 8, "State": "in-use", "Tags": tags(Project=P)},
        {"VolumeId": "vol-3", "Size": 8, "State": "in-use", "Tags": tags(Campaign="iot-a", Scope="pv", Project=P)},
        {"VolumeId": "vol-4", "Size": 8, "State": "in-use", "Tags": tags(Campaign="ec-a", Scope="pv", Project="other-team")}]}],
    "describe_vpcs": [{"Vpcs": [{"VpcId": "vpc-1", "Tags": tags(Campaign="ec-a", Scope="network", Project=P)}]}],
}, direct={"describe_addresses": {"Addresses": []}})
f = sweep.ec2_findings(ec2, P, None, False)
check("sweep lists this project's campaign volumes, skips untagged and other teams' (M2)", sorted(x.ident for x in f) == ["vol-1", "vol-3"], f)
def _flt(kw):
    return kw.get("Filters", kw.get("Filter", []))


check("sweep asks EC2 for tag:Project only (server-side filter)", all(any(fl["Name"] == "tag:Project" and fl["Values"] == [P] for fl in _flt(kw))
      or any(fl["Name"].startswith("tag:cluster.k8s.amazonaws.com") for fl in _flt(kw)) for _, kw in ec2.calls), [c for c in ec2.calls if not any(fl["Name"] == "tag:Project" for fl in _flt(c[1]))])
f = sweep.ec2_findings(ec2, P, "ec-a", False)
check("sweep filters by campaign", [x.ident for x in f] == ["vol-1"], f)
f = sweep.ec2_findings(ec2, P, None, True)
check("sweep hides shared/network scope unless asked", "vpc-1" in [x.ident for x in f] and "vpc-1" not in [x.ident for x in sweep.ec2_findings(ec2, P, None, False)])
cni = FakeClient({"describe_network_interfaces": [{"NetworkInterfaces": [{"NetworkInterfaceId": "eni-9", "Status": "available",
      "TagSet": tags(**{"cluster.k8s.amazonaws.com/name": P + "-ec-a"})}]}]}, direct={"describe_addresses": {"Addresses": []}})
check("sweep reports VPC CNI interfaces left behind (no Campaign tag, M5)", [(x.kind, x.ident, x.campaign) for x in sweep.ec2_findings(cni, P, "ec-a", False)]
      == [("ec2:network-interface(cni)", "eni-9", "ec-a")])
eksf = RecordingClient({"list_clusters": [{"clusters": ["someone-elses", P + "-ec-a"]}], "list_nodegroups": [{"nodegroups": ["kafka"]}]},
                       direct={"describe_cluster": {"cluster": {"status": "ACTIVE", "tags": {"Project": P, "Campaign": "ec-a"}}}})
described = []
eksf.describe_cluster = lambda **kw: (described.append(kw["name"]), {"cluster": {"status": "ACTIVE", "tags": {"Project": P, "Campaign": "ec-a"}}})[1]
f = sweep.eks_findings(eksf, P, None, False)
check("sweep never describes another team's EKS cluster", described == [P + "-ec-a"] and {x.ident for x in f} == {P + "-ec-a", P + "-ec-a/kafka"}, (described, f))
check("sweep has no load balancer listing of its own (the tagging API filters on Project)", not hasattr(sweep, "elb_findings"))
check("sweep has no mutating call", not any(n.startswith(("delete", "terminate", "modify", "put", "create", "update")) for n in dir(sweep) if n.endswith("_findings")) )
src = Path(sweep.__file__).read_text()
check("sweep source calls no mutating API", not any(w in src for w in (".delete_", ".terminate_", ".modify_", ".put_", ".create_", ".update_", ".detach_")))
ec2v = FakeClient({"describe_volumes": [{"Volumes": [{"VolumeId": "vol-9", "Size": 300, "State": "available"}, {"VolumeId": "vol-8", "Size": 1, "State": "deleting"}]}]})
check("teardown volume check ignores volumes already deleting", [v["VolumeId"] for v in tg.volumes_left(ec2v, "ec-a")] == ["vol-9"])
src_tg = Path(tg.__file__).read_text()
check("teardown volume check filters on Campaign AND Scope=pv", '"tag:Scope", "Values": ["pv"]' in src_tg and '"tag:Campaign"' in src_tg)
check("discover overlap math", discover.overlaps("10.123.0.0/16", ["10.0.0.0/16", "10.123.5.0/24"]) == ["10.123.5.0/24"] and discover.overlaps("10.200.0.0/16", ["10.0.0.0/16"]) == [])

# ------------------------------------------------------------------ discover: vCPU use and account settings (REVIEW4 C5, M6)
cw, sq, ec2s, kms, gd, iam, sts = (sess.client(n) for n in ("cloudwatch", "service-quotas", "ec2", "kms", "guardduty", "iam", "sts"))
NOW = dt.datetime(2026, 10, 14, 12, 0, tzinfo=dt.timezone.utc)
dims = [{"Name": "Service", "Value": "EC2"}, {"Name": "Resource", "Value": "vCPU"}, {"Name": "Type", "Value": "Resource"},
        {"Name": "Class", "Value": "Standard/OnDemand"}]
with Stubber(cw) as st:
    st.add_response("get_metric_statistics", {"Datapoints": [{"Timestamp": NOW - dt.timedelta(minutes=10), "Maximum": 40.0},
                                                             {"Timestamp": NOW - dt.timedelta(minutes=5), "Maximum": 48.0}]},
                    {"Namespace": "AWS/Usage", "MetricName": "ResourceCount", "Dimensions": dims, "StartTime": NOW - dt.timedelta(hours=1),
                     "EndTime": NOW, "Period": 300, "Statistics": ["Maximum"]})
    check("vCPU use is the latest AWS/Usage ResourceCount datapoint", discover.vcpu_in_use(cw, NOW) == 48.0)
check("vCPU: free below the need BLOCKS", discover.vcpu_check(96, 48, 72)[0] == "BLOCK")
check("vCPU: enough headroom passes", discover.vcpu_check(256, 48, 72)[0] == "ok")
check("vCPU: unknown use asks for a decision", discover.vcpu_check(256, None, 72)[0] == "DECIDE")
with Stubber(sq) as st:
    st.add_client_error("get_service_quota", "NoSuchResourceException", expected_params={"ServiceCode": "ec2", "QuotaCode": "L-1216C47A"})
    st.add_response("get_aws_default_service_quota", {"Quota": {"Value": 5.0}}, {"ServiceCode": "ec2", "QuotaCode": "L-1216C47A"})
    check("a never-changed quota falls back to the AWS default (no more None)", discover.quota(sq, "ec2", "L-1216C47A") == (5.0, "AWS default"))
with Stubber(ec2s) as st:
    st.add_response("get_ebs_encryption_by_default", {"EbsEncryptionByDefault": True})
    st.add_response("get_ebs_default_kms_key_id", {"KmsKeyId": "alias/aws/ebs"})
    check("default EBS key alias/aws/ebs passes", discover.ebs_key_check(ec2s, kms)[0] == "ok")
with Stubber(ec2s) as st, Stubber(kms) as sk:
    key = "arn:aws:kms:us-east-2:000000000000:key/11111111-2222-3333-4444-555555555555"
    st.add_response("get_ebs_encryption_by_default", {"EbsEncryptionByDefault": True})
    st.add_response("get_ebs_default_kms_key_id", {"KmsKeyId": key})
    sk.add_response("describe_key", {"KeyMetadata": {"KeyId": "11111111-2222-3333-4444-555555555555", "KeyManager": "CUSTOMER"}}, {"KeyId": key})
    check("a customer-managed default EBS key BLOCKS", discover.ebs_key_check(ec2s, kms)[0] == "BLOCK")
with Stubber(gd) as st:
    st.add_response("list_detectors", {"DetectorIds": ["d1"]})
    st.add_response("get_detector", {"ServiceRole": "r", "Status": "ENABLED", "Features": [{"Name": "RUNTIME_MONITORING", "Status": "ENABLED",
                    "AdditionalConfiguration": [{"Name": "EKS_ADDON_MANAGEMENT", "Status": "ENABLED"}]}]}, {"DetectorId": "d1"})
    check("GuardDuty Runtime Monitoring with EKS agents asks for a decision", discover.guardduty_check(gd)[0] == "DECIDE")
with Stubber(gd) as st:
    st.add_response("list_detectors", {"DetectorIds": []})
    check("no GuardDuty detector passes", discover.guardduty_check(gd)[0] == "ok")
with Stubber(sq) as st:
    st.add_response("get_service_quota", {"Quota": {"Value": 2.0}}, {"ServiceCode": "ebs", "QuotaCode": "L-7A658B76"})
    check("gp3 quota below 2 clusters x 1.6 TiB BLOCKS", discover.storage_check(sq, 2)[0] == "BLOCK")
with Stubber(sts) as ss, Stubber(iam) as si:
    ss.add_response("get_caller_identity", {"Arn": "arn:aws:sts::000000000000:assumed-role/Operator/session", "Account": "000000000000", "UserId": "x"})
    si.add_response("get_role", {"Role": {"Path": "/team/", "RoleName": "Operator", "RoleId": "AROAEXAMPLEEXAMPLE1", "CreateDate": NOW,
                    "Arn": "arn:aws:iam::000000000000:role/team/Operator"}}, {"RoleName": "Operator"})
    si.add_response("simulate_principal_policy", {"EvaluationResults": [{"EvalActionName": "iam:CreateRole", "EvalDecision": "implicitDeny"},
                    {"EvalActionName": "eks:CreateCluster", "EvalDecision": "allowed"}]},
                    {"PolicySourceArn": "arn:aws:iam::000000000000:role/team/Operator", "ActionNames": ["iam:CreateRole", "iam:PutRolePolicy", "iam:TagRole", "eks:CreateCluster"],
                     "ContextEntries": [{"ContextKeyName": "aws:MultiFactorAuthPresent", "ContextKeyValues": ["true"], "ContextKeyType": "boolean"}]})
    lvl, msg = discover.iam_check(sts, iam)
    check("IAM: an assumed role is simulated as its role (with path); a denied CreateRole BLOCKS", lvl == "BLOCK" and "iam:CreateRole" in msg, msg)

# ------------------------------------------------------------------ eks_api_cidrs (REVIEW4 C3)
check("CIDR rules: /32 accepted", eks_api_cidrs.check_cidrs(["203.0.113.9/32"]) == ["203.0.113.9/32"])
for bad in (["0.0.0.0/0"], ["10.0.0.0/8"], ["203.0.113.9/33"], ["2001:db8::/64"], []):
    try:
        eks_api_cidrs.check_cidrs(bad); check(f"CIDR rules refuse {bad}", False)
    except SystemExit:
        check(f"CIDR rules refuse {bad}", True)
CL = {"name": P + "-pilot-ec", "tags": {"Project": P, "Campaign": "pilot-ec"},
      "resourcesVpcConfig": {"endpointPublicAccess": True, "endpointPrivateAccess": True, "publicAccessCidrs": ["198.51.100.7/32"]}}


class A:  # argparse stand-in
    cluster_name, campaign, project, wait_update, timeout, dry_run = P + "-pilot-ec", "pilot-ec", P, None, 0, False
    cidr = ["203.0.113.9/32"]


with Stubber(eks) as st:
    st.add_response("describe_cluster", {"cluster": CL}, {"name": P + "-pilot-ec"})
    st.add_response("update_cluster_config", {"update": {"id": "u-1", "status": "InProgress"}},
                    {"name": P + "-pilot-ec", "resourcesVpcConfig": {"endpointPublicAccess": True, "endpointPrivateAccess": True, "publicAccessCidrs": ["203.0.113.9/32"]}})
    st.add_response("describe_update", {"update": {"id": "u-1", "status": "Successful"}}, {"name": P + "-pilot-ec", "updateId": "u-1"})
    check("C3: update_cluster_config with the new CIDRs, flags unchanged, waits for Successful", eks_api_cidrs.run(eks, A) == 0)
with Stubber(eks) as st:
    st.add_response("describe_cluster", {"cluster": CL}, {"name": P + "-pilot-ec"})
    st.add_response("update_cluster_config", {"update": {"id": "u-2", "status": "InProgress"}},
                    {"name": P + "-pilot-ec", "resourcesVpcConfig": {"endpointPublicAccess": True, "endpointPrivateAccess": True, "publicAccessCidrs": ["203.0.113.9/32"]}})
    st.add_response("describe_update", {"update": {"id": "u-2", "status": "InProgress"}}, {"name": P + "-pilot-ec", "updateId": "u-2"})
    check("C3: still InProgress after the timeout exits 3", eks_api_cidrs.run(eks, A) == 3)
with Stubber(eks) as st:
    st.add_response("describe_cluster", {"cluster": {**CL, "tags": {"Project": "other"}}}, {"name": P + "-pilot-ec"})
    try:
        eks_api_cidrs.run(eks, A); check("C3: a cluster of another project is refused", False)
    except SystemExit:
        check("C3: a cluster of another project is refused", True)

# ------------------------------------------------------------------ network_guard (REVIEW4 M5)
class G:
    def __init__(self, data, cluster): self.data, self.cluster = data, cluster
    def get_paginator(self, op): return FakePaginator(self.data.get(op, [{}]))
    def describe_cluster(self, name): return {"cluster": self.cluster[name]}


eksg = G({"list_clusters": [{"clusters": ["other-team-x", P + "-ec-a"]}]}, {P + "-ec-a": {"status": "ACTIVE", "resourcesVpcConfig": {"vpcId": "vpc-1"}}})
ec2g = G({"describe_vpc_endpoints": [{"VpcEndpoints": [{"VpcEndpointId": "vpce-s3", "Tags": tags(Project=P), "NetworkInterfaceIds": []},
                                                       {"VpcEndpointId": "vpce-gd", "ServiceName": "com.amazonaws.us-east-2.guardduty-data", "Tags": tags(GuardDutyManaged="true")}]}],
          "describe_network_interfaces": [{"NetworkInterfaces": [{"NetworkInterfaceId": "eni-nat", "InterfaceType": "nat_gateway"},
                                                                 {"NetworkInterfaceId": "eni-cni", "InterfaceType": "interface", "Status": "available", "Description": "aws-K8S-i-1"}]}]}, {})
found = network_guard.problems(eksg, ec2g, "vpc-1", P)
check("network guard: a live project cluster in the VPC refuses", any("still uses vpc-1" in x for x in found), found)
check("network guard: a GuardDuty endpoint is named", any("vpce-gd" in x and "GuardDuty" in x for x in found), found)
check("network guard: a stray CNI interface is named, the NAT interface is not", any("eni-cni" in x for x in found) and not any("eni-nat" in x for x in found), found)
check("network guard: an empty VPC passes", network_guard.problems(G({}, {}), G({}, {}), "vpc-1", P) == [])

# ------------------------------------------------------------------ delete_orphan_volumes (REVIEW4 M4 item 1)
from botocore.exceptions import ClientError  # noqa: E402


class EksGone:
    def describe_cluster(self, name):
        raise ClientError({"Error": {"Code": "ResourceNotFoundException", "Message": "x"}}, "DescribeCluster")


class EksThere:
    def describe_cluster(self, name): return {"cluster": {"name": name}}


class Ec2Del(FakeClient):
    def __init__(self, data):
        super().__init__(data); self.deleted = []
    def delete_volume(self, VolumeId): self.deleted.append(VolumeId)


vd = Ec2Del({"describe_volumes": [{"Volumes": [{"VolumeId": "vol-7", "Size": 100}]}]})
check("orphans: refused while the cluster exists", dov.run(vd, EksThere(), P, "ec-a", True) == 1 and vd.deleted == [])
check("orphans: without --yes nothing is deleted", dov.run(vd, EksGone(), P, "ec-a", False) == 0 and vd.deleted == [])
check("orphans: with --yes exactly the listed volume is deleted", dov.run(vd, EksGone(), P, "ec-a", True) == 0 and vd.deleted == ["vol-7"])
src_dov = Path(dov.__file__).read_text()
check("orphans: the filter needs Project, Campaign, Scope=pv and status available", all(x in src_dov for x in ('"tag:Project"', '"tag:Campaign"', '"tag:Scope", "Values": ["pv"]', '"status", "Values": ["available"]')))

sys.exit(1 if failures else 0)
