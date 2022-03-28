variable "project" {
  type = string
}

variable "credentials_file" {
  default = "~/.google/credentials/google_credentials.json"
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-a"
}

// Cloud Storage config
variable "storage_class" {
  description = "Storage class of a Google Cloud Storage bucket"
  default     = "STANDARD"
}

variable "data_bucket_name" {
  description = "Name of the GCS bucket to store GitHub event data"
  default     = "github_archive_data"
}

// BQ vars
variable "dataset_id" {
  default = "src_github"
}
