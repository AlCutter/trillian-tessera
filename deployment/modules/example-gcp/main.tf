terraform {
  backend "gcs" {}
}

module "gcp" {
  source = "../"
}

###
### Set up Cloud Run service
###
resource "google_service_account" "cloudrun_service_account" {
  account_id   = "cloudrun-${var.env}-sa"
  display_name = "Service Account for Cloud Run (${var.env})"
}

resource "google_project_iam_member" "iam_act_as" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.cloudrun_service_account.email}"
}
resource "google_project_iam_member" "iam_metrics_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.cloudrun_service_account.email}"
}
resource "google_project_iam_member" "iam_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.cloudrun_service_account.email}"
}
resource "google_project_iam_member" "iam_service_agent" {
  project = var.project_id
  role    = "roles/run.serviceAgent"
  member  = "serviceAccount:${google_service_account.cloudrun_service_account.email}"
}
resource "google_project_iam_member" "iam_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.cloudrun_service_account.email}"
}

resource "google_cloud_run_v2_service" "default" {
  name         = "example-service-${var.env}"
  location     = var.location
  launch_stage = "GA"

  template {
    service_account = google_service_account.cloudrun_service_account.email
    containers {
      image = var.example_gcp_docker_image
      name  = "example-gcp"
      args = [
        "--logtostderr",
        "--v=1",
        "--bucket=${modules.infra.outputs.log_bucket}",
        "--spanner=${modules.infra.outputs.log_spanner}",
        "--project=${var.project_id}",
        "--signer=./testgcp.sec",
      ]
      ports {
        container_port = 8080
      }

      startup_probe {
        initial_delay_seconds = 1
        timeout_seconds       = 1
        period_seconds        = 10
        failure_threshold     = 3
        tcp_socket {
          port = 8080
        }
      }
    }
    containers {
      image      = "us-docker.pkg.dev/cloud-ops-agents-artifacts/cloud-run-gmp-sidecar/cloud-run-gmp-sidecar:1.0.0"
      name       = "collector"
      depends_on = ["example-gcp"]
    }
  }
  client = "terraform"
  depends_on = [
    google_project_service.secretmanager_api,
    google_project_service.spanner_api,
    google_project_iam_member.iam_act_as,
    google_project_iam_member.iam_metrics_writer,
    google_project_iam_member.iam_spanner_client,
    google_project_iam_member.iam_service_agent,
    google_project_iam_member.iam_secret_accessor,
  ]
}

resource "google_cloud_run_service_iam_binding" "default" {
  location = google_cloud_run_v2_service.default.location
  service  = google_cloud_run_v2_service.default.name
  role     = "roles/run.invoker"
  members = [
    "allUsers"
  ]
}
