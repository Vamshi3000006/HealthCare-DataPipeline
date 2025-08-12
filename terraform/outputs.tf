output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "processed_bucket" {
  value = google_storage_bucket.processed.name
}

output "logs_bucket" {
  value = google_storage_bucket.logs.name
}
