resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member = "serviceAccount:terraform-admin@macro-landing-462401-g0.iam.gserviceaccount.com"
  lifecycle {
  prevent_destroy = true
        }
}

resource "google_service_account" "nifi" {
  account_id   = "nifi-sa"
  display_name = "NiFi Service Account"
}

resource "google_project_iam_member" "nifi_gcs_writer" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.nifi.email}"
}

resource "google_service_account" "spark" {
  account_id   = "spark-sa"
  display_name = "Spark Service Account"
}

resource "google_project_iam_member" "spark_roles" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_project_iam_member" "spark_gcs" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_service_account" "dbt" {
  account_id   = "dbt-sa"
  display_name = "dbt Service Account"
}

resource "google_project_iam_member" "dbt_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}


