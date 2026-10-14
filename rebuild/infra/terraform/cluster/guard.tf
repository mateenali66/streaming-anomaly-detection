# ------------------------------------------------ teardown guard (REVIEW3 A1, A5)
# Created LAST, so Terraform destroys it FIRST, while the cluster, the CSI driver and the operators still exist.
# scripts/pre_destroy.sh:
#   1. aborts unless s3://<results bucket>/campaign/<id>/COMPLETE exists and is newer than every result object;
#   2. deletes chaos resources, Flink, Kafka and all PVCs while the operators and the CSI driver still run;
#   3. proves no PV exists and no EBS volume tagged Campaign=<id> remains.
# There is no override flag. To discard data on purpose, delete the cluster by hand and say so in the log.
resource "terraform_data" "teardown_guard" {
  input = {
    bucket   = local.results_bucket
    campaign = var.campaign_id
    region   = var.region
    cluster  = module.eks.cluster_name
    script   = abspath("${path.module}/../../scripts/pre_destroy.sh")
    python   = var.python_bin
    # coalesce() fails when every argument is null or empty, which stopped every plan with aws_profile = null (REVIEW4 B1)
    profile = var.aws_profile == null ? "" : var.aws_profile
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = "\"$GUARD_SCRIPT\" \"$GUARD_BUCKET\" \"$GUARD_CAMPAIGN\" \"$GUARD_REGION\" \"$GUARD_CLUSTER\""
    environment = {
      GUARD_SCRIPT   = self.input.script
      GUARD_BUCKET   = self.input.bucket
      GUARD_CAMPAIGN = self.input.campaign
      GUARD_REGION   = self.input.region
      GUARD_CLUSTER  = self.input.cluster
      PYTHON_BIN     = self.input.python
      GUARD_PROFILE  = self.input.profile # empty means: keep the AWS_PROFILE already exported
    }
  }

  # Everything pre_destroy.sh needs must be destroyed AFTER the guard, so the guard depends on all of it. The four inline
  # role policies are listed on purpose: nothing else depends on them, so without these lines a destroy deletes them in
  # the first wave, in parallel with the guard. A refusing guard then leaves the cluster without S3, SNS and EBS
  # permissions, and a passing guard cannot delete the PV volumes (REVIEW4 B3). tests/check_tf_graph.py checks that every
  # resource of this root is reachable from the guard.
  depends_on = [
    module.eks,
    aws_iam_role_policy.ebs_csi,
    aws_iam_role_policy.orchestrator,
    aws_iam_role_policy.exporter,
    aws_iam_role_policy.flink,
    helm_release.strimzi,
    helm_release.flink_operator,
    helm_release.prometheus,
    helm_release.chaos_mesh,
    kubectl_manifest.gp3,
    aws_eks_pod_identity_association.orchestrator,
    aws_eks_pod_identity_association.exporter,
    aws_eks_pod_identity_association.flink,
  ]
}
