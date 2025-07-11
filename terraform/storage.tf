resource "google_storage_bucket" "raw" {
  name     = "healthcare-raw-bucket"
  location = "US"
}

resource "google_storage_bucket" "processed" {
  name     = "healthcare-processed-bucket"
  location = "US"
}

resource "google_storage_bucket" "logs" {
  name     = "healthcare-logs-bucket"
  location = "US"
}
resource "google_storage_bucket" "tf_state" {
  name     = "healthcare-terraform-state"
  location = "US"
  versioning {
    enabled = true
  }
}
