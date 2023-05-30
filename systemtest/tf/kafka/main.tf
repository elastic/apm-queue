provider "helm" {
  kubernetes {
    config_path = var.kube_config_path
  }
}

provider "kubernetes" {
  config_path = var.kube_config_path
}

# creating namespace here so it is also cleaned up on destroy
resource "kubernetes_namespace" "kafka" {
  metadata {
    name = var.namespace
  }
}

locals {
  topics_value = "{${join(",", var.topics)}}"
  wait_timeout = "1800s"
}

resource "helm_release" "strimzi" {
  name             = "strimzi"
  repository       = "https://strimzi.io/charts/"
  chart            = "strimzi-kafka-operator"
  namespace        = var.namespace
  wait             = true
  create_namespace = true
}

resource "helm_release" "kafka" {
  name      = "${var.namespace}-kafka"
  namespace = kubernetes_namespace.kafka.metadata.0.name
  chart     = "../../../infra/k8s/kafka"

  depends_on = [helm_release.strimzi]

  set {
    name  = "topics"
    value = local.topics_value
  }
  set {
    name  = "cluster"
    value = var.name
  }
  set {
    name  = "namespace"
    value = var.namespace
  }
  wait             = true
  create_namespace = true
}

resource "null_resource" "kafka_ready" {
  triggers = {
    topics    = local.topics_value
    namespace = var.namespace
  }

  depends_on = [helm_release.kafka]

  provisioner "local-exec" {
    # Ensure that the kafka topics have been provisioned.
    command = "kubectl -n ${var.namespace} wait --timeout=${local.wait_timeout} --for=condition=Ready=True kafkatopics ${var.topics[0]}"
  }
}

#
# Vars
#

variable "kube_config_path" {
  default     = "~/.kube/config"
  type        = string
  description = "the path of the kube config file"
}

variable "namespace" {
  default     = "kafka"
  type        = string
  description = "the namespace where to provision the Kafka operator and cluster"
}

variable "name" {
  default     = "kafka"
  type        = string
  description = "the name to use for the Kafka cluster"
}

variable "topics" {
  type        = list(string)
  description = "the list of topics that will be created by the operator"
}

#
# Outputs
#

output "deployment_type" {
  value       = "k8s"
  description = "the deployment type"
}

output "kafka_brokers" {
  value       = ["localhost:9093"]
  description = "the list of brokers to use to connect"
}
