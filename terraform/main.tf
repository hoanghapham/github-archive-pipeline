terraform {
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.5.0"
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
resource "google_bigquery_dataset" "src_github_events" {
  dataset_id                 = "src_github_events"
  project                    = var.project
  delete_contents_on_destroy = true

  access {
    role          = "OWNER"
    user_by_email = var.owner_email
  }
}

// Cloud Storage
resource "google_storage_bucket" "github_archive" {
  name     = "${var.project}_raw_github_archive"
  location = var.region
  project = var.project

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