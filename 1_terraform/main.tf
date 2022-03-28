terraform {
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.15.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project
  region      = var.region
  zone        = var.zone
}

# BigQuery Data warehouse
resource "google_bigquery_dataset" "src_github" {
  dataset_id                 = var.dataset_id
  project                    = var.project
  delete_contents_on_destroy = true

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
}

// Cloud Storage
resource "google_storage_bucket" "github_archive" {
  name     = "${var.project}_${var.data_bucket_name}"
  location = var.region
  project  = var.project

  storage_class = var.storage_class

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = 30 // day
    }
  }

  force_destroy = true
}