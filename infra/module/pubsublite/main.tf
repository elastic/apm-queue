locals {
  suffix      = var.suffix
  region      = var.region
  reservation = var.reservation
  topics      = toset(var.topics)
}

resource "google_pubsub_lite_topic" "topic" {
  for_each = local.topics

  name    = "${var.prefix}.${each.key}${local.suffix}"
  project = data.google_project.project.number

  partition_config {
    count = var.partition_count
    capacity {
      // 4MiB is the minimum allowed throughput per partition.
      publish_mib_per_sec   = 4
      subscribe_mib_per_sec = 4
    }
  }

  retention_config {
    period = "3600s" // 1h.

    per_partition_bytes = 32212254720 // Minimum
  }

  reservation_config {
    throughput_reservation = "projects/${data.google_project.project.number}/locations/${local.region}/reservations/${local.reservation}"
  }
}

resource "google_pubsub_lite_subscription" "topic" {
  for_each = var.create_subscription ? local.topics : []

  name  = "${var.prefix}.${each.key}${local.suffix}"
  topic = google_pubsub_lite_topic.topic[each.key].name
  delivery_config {
    delivery_requirement = "DELIVER_AFTER_STORED"
  }
}

data "google_project" "project" {}

# REQUIRED variables

variable "prefix" {
  description = "Required resource topic prefix"
  type        = string
}

variable "topics" {
  description = "Required topics to create, the format will be {prefix}.{topic}{suffix}"
  type        = list(string)
}

variable "reservation" {
  description = "Required existing reservation to use for ALL the topics"
  type        = string
}

# OPTIONAL variables

variable "suffix" {
  description = "Optional topic suffix"
  default     = ""
  type        = string
}

variable "create_subscription" {
  description = "Whether or not to create a subscription for each of the topics. The subscription name wil be the same as the topic name"
  default     = false
  type        = bool
}

variable "region" {
  description = "Optional GCP region"
  default     = "asia-south1"
  type        = string
}

variable "partition_count" {
  description = "Optional partitions per topic"
  default     = 1
  type        = number
}
