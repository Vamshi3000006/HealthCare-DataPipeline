resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member = "serviceAccount:terraform-admin@macro-landing-462401-g0.iam.gserviceaccount.com"
}
