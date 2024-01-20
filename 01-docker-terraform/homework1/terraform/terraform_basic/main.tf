terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.12.0"
    }
  }
}

provider "google" {
  # Configuration options
#   credentials = "../keys/my_cred.json"
  project = "terraform-demo-411721"
  region  = "us-central1"

}

resource "google_storage_bucket" "demo_bucket" {
  name          = "terraform-demo-411721-terra-bucket"
  location      = "US"
  force_destroy = true

  
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}