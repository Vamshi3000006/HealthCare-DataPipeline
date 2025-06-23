provider "google" {
  credentials = file("terraform-sa-key.json")
  project     = "macro-landing-462401-g0"
  region      = "us-central1"
}

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

