locals {
  prefix      = "systemtest"
  reservation = "${local.prefix}-${var.suffix}"
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.region // Since the topic is zonal, use the same value.
}

module "pubsublite" {
  source = "../../../infra/module/pubsublite"
  region = var.region
  topics = var.topics
  prefix = "" // No prefixes for topics, they'pre suffixed in the code.

  reservation = google_pubsub_lite_reservation.reservation.name

  create_subscription = true
}

resource "google_pubsub_lite_reservation" "reservation" {
  name                = local.reservation
  project             = data.google_project.project.number
  throughput_capacity = var.topics == null ? 1 : length(var.topics) * 2
}

data "google_project" "project" {}

variable "region" {
  type        = string
  description = "the GCP region where to create the resources"
}

variable "project" {
  type        = string
  description = "the GCP project where to create the resources"
}

variable "suffix" {
  description = "the resource suffix"
  type        = string
}

variable "topics" {
  description = "the topics to create"
  type        = list(string)
}
