variable "project" {
  default = "personal-39217"
}

variable "credentials_file" {
  default = "/home/hapham/.google/credentials/google_credentials.json"
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-a"
}

variable "owner_email" {
  default = "hoangha.20021992@gmail.com"
}

// Cloud Storage config
variable "storage_class" {
  description = "Storage class of a Google Cloud Storage bucket"
  default     = "STANDARD"
}

// BQ vars
variable "dataset_id" {
  default = "src_github_events"
}