# ------------------------------------------------ Kubernetes layer
# Terraform installs only the platform: namespaces, storage class, priority class and the four operator charts.
# Workload objects (Kafka, topics, Flink, load generator, orchestrator) are applied by scripts/deploy_workload.sh
# from k8s/ after this root has finished, so a failed workload never blocks or half-destroys the cluster.

locals {
  images = yamldecode(file("${path.module}/../../images/images.lock.yaml"))

  all_images = merge(
    { for k, v in local.images.built : k => v },
    { for m in local.images.mirrored : m.key => m },
  )

  # Variables available to every values file as ${NAME}. scripts/lock_env.py produces the same names for envsubst.
  image_vars = merge(
    {
      ECR_REGISTRY = local.ecr_registry
      ECR_PREFIX   = var.project
      CAMPAIGN_ID  = var.campaign_id
      WORKLOAD     = var.workload
      AWS_REGION   = var.region
    },
    { for k, v in local.all_images : "TAG_${upper(replace(k, "-", "_"))}" => v.tag },
    { for k, v in local.all_images : "DIGEST_${upper(replace(k, "-", "_"))}" => v.digest },                                       # sha256:<hex>
    { for k, v in local.all_images : "SHA_${upper(replace(k, "-", "_"))}" => trimprefix(v.digest, "sha256:") },                   # <hex>
    { for k, v in local.all_images : "PATH_${upper(replace(k, "-", "_"))}" => "${var.project}/${v.repo}" },                       # repository path inside the registry
    { for k, v in local.all_images : "REPO_${upper(replace(k, "-", "_"))}" => "${local.ecr_registry}/${var.project}/${v.repo}" }, # registry + path
    { for k, v in local.all_images : "REF_${upper(replace(k, "-", "_"))}" => "${local.ecr_registry}/${var.project}/${v.repo}:${v.tag}@${v.digest}" },
  )

  exec_args = concat(
    [abspath("${path.module}/../../scripts/eks_token.py"), "--cluster-name", module.eks.cluster_name, "--region", var.region],
    var.aws_profile == null ? [] : ["--profile", var.aws_profile],
  )
}

resource "terraform_data" "assert_image_digests" {
  count = var.enforce_image_digests ? 1 : 0
  lifecycle {
    precondition {
      condition     = alltrue([for k, v in local.all_images : can(regex("^sha256:[0-9a-f]{64}$", v.digest)) && v.tag != ""])
      error_message = "images.lock.yaml still has an empty or malformed digest or tag. Run scripts/mirror_images.py and scripts/build_images.sh first (README, runbook steps 6 to 8)."
    }
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = var.python_bin
      args        = local.exec_args
    }
  }
}

provider "kubectl" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  load_config_file       = false
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = var.python_bin
    args        = local.exec_args
  }
}

resource "kubectl_manifest" "ns" {
  for_each = {
    streaming     = "kafka-flink-pipeline"
    observability = "monitoring"
    "chaos-mesh"  = "fault-injection"
  }
  yaml_body  = yamlencode({ apiVersion = "v1", kind = "Namespace", metadata = { name = each.key, labels = { purpose = each.value } } })
  depends_on = [module.eks]
}

resource "kubectl_manifest" "gp3" {
  yaml_body = yamlencode({
    apiVersion           = "storage.k8s.io/v1"
    kind                 = "StorageClass"
    metadata             = { name = "gp3", annotations = { "storageclass.kubernetes.io/is-default-class" = "true" } }
    provisioner          = "ebs.csi.aws.com"
    parameters           = { type = "gp3", iops = "3000", throughput = "125", encrypted = "true" }
    reclaimPolicy        = "Delete" # keep Delete. The teardown guard proves the volumes are gone.
    volumeBindingMode    = "WaitForFirstConsumer"
    allowVolumeExpansion = true
  })
  depends_on = [module.eks]
}

# The values file names this class, so it must exist BEFORE the Prometheus chart (REVIEW3 A4).
resource "kubectl_manifest" "monitoring_critical" {
  yaml_body = yamlencode({
    apiVersion    = "scheduling.k8s.io/v1"
    kind          = "PriorityClass"
    metadata      = { name = "monitoring-critical" }
    value         = 1000000
    globalDefault = false
    description   = "Prometheus must not be evicted during fault injection"
  })
  depends_on = [module.eks]
}

resource "helm_release" "strimzi" {
  name       = "strimzi"
  repository = "oci://quay.io/strimzi-helm"
  chart      = "strimzi-kafka-operator"
  version    = var.chart_versions.strimzi
  namespace  = "streaming"
  timeout    = 600
  values     = [templatefile("${path.module}/../../k8s/base/helm-values/strimzi-values.yaml", local.image_vars)]
  depends_on = [kubectl_manifest.ns, terraform_data.assert_image_digests]
}

resource "helm_release" "flink_operator" {
  name       = "flink-operator"
  repository = "https://archive.apache.org/dist/flink/flink-kubernetes-operator-${var.chart_versions.flink_operator}/"
  chart      = "flink-kubernetes-operator"
  version    = var.chart_versions.flink_operator
  namespace  = "streaming"
  timeout    = 600
  values     = [templatefile("${path.module}/../../k8s/base/helm-values/flink-operator-values.yaml", local.image_vars)]
  depends_on = [kubectl_manifest.ns, terraform_data.assert_image_digests]
}

resource "helm_release" "prometheus" {
  name       = "prometheus"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = var.chart_versions.kube_prometheus_stack
  namespace  = "observability"
  timeout    = 900
  wait       = true
  values     = [templatefile("${path.module}/../../k8s/base/helm-values/prometheus-values.yaml", local.image_vars)]
  depends_on = [kubectl_manifest.ns, kubectl_manifest.gp3, kubectl_manifest.monitoring_critical, terraform_data.assert_image_digests]
}

resource "helm_release" "chaos_mesh" {
  name       = "chaos-mesh"
  repository = "https://charts.chaos-mesh.org"
  chart      = "chaos-mesh"
  version    = var.chart_versions.chaos_mesh
  namespace  = "chaos-mesh"
  timeout    = 600
  values     = [templatefile("${path.module}/../../k8s/base/helm-values/chaos-mesh-values.yaml", local.image_vars)]
  depends_on = [kubectl_manifest.ns, terraform_data.assert_image_digests]
}
