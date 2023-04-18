provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
}

locals {
  topics_value = "{${join(",", var.topics)}}"
}

resource "helm_release" "strimzi" {
  name             = "strimzi"
  repository       = "https://strimzi.io/charts/"
  chart            = "strimzi-kafka-operator"
  namespace        = var.namespace
  wait             = true
  create_namespace = true
}

variable "namespace" {
  default     = "kafka"
  type        = string
  description = "the namespace where to provision the Kafka operator and cluster"
}

resource "helm_release" "kafka" {
  name      = "kafka"
  namespace = var.namespace
  chart     = "../../../infra/k8s/kafka"

  depends_on = [helm_release.strimzi]

  set {
    name  = "topics"
    value = local.topics_value
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
    command = "kubectl -n ${var.namespace} wait --timeout=${var.wait_timeout} --for=condition=Ready=True kafkatopics ${var.topics[0]}"
  }
}

variable "topics" {
  type        = list(string)
  description = "the list of topics that will be created by the operator"
}


variable "wait_timeout" {
  type        = string
  description = "the amount of time to wait until the topic is available"
  default     = "240s"
}
