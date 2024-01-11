terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.49.0"
    }
  }
}

provider "google" {
  # Configuration options
}

variable "folder_id" {
  type = string
}

variable "backend_prefix" {
  type = string
}

variable "billing_account_id" {
  type = string
}

locals {
  backend_folder_display_name = "${var.backend_prefix}-bq-driver-folder"
  service_project_name = "main-${var.backend_prefix}-bq-driver"
  service_project_id = "${var.backend_prefix}-bq-driver"
  service_account_id = "${var.backend_prefix}-main-service-acc"
}

variable services {
  type        = list
  default     = [
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "iam.googleapis.com",
    "cloudbilling.googleapis.com",
    "analyticshub.googleapis.com",
    "bigquery.googleapis.com",
  ]
}

data "google_folder" "folder" {
  folder = "folders/${var.folder_id}"
}

resource "google_project" "service_project" {
  name            = local.service_project_name
  project_id      = local.service_project_id
  folder_id       = data.google_folder.folder.folder_id
  billing_account = var.billing_account_id
}

resource "google_project_service" "services" {
  for_each = toset(var.services)
  project                    = google_project.service_project.project_id
  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
  depends_on = [google_project.service_project]
}

resource "google_service_account" "service_account" {
  account_id = local.service_account_id
  description = "Service account to managing keboola backend projects"
  project = google_project.service_project.project_id
}

resource "google_folder_iam_member" "folder_service_acc_project_creator_role" {
  folder  = data.google_folder.folder.name
  role    = "roles/resourcemanager.projectCreator"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_folder_iam_member" "folder_service_acc_project_list_role" {
  folder  = data.google_folder.folder.name
  role    = "roles/browser"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_billing_account_iam_member" "billing_acc_binding" {

  billing_account_id = var.billing_account_id
  member             = "serviceAccount:${google_service_account.service_account.email}"
  role               = "roles/billing.user"
}

output "service_project_id" {
  value = google_project.service_project.project_id
}

resource "google_storage_bucket" "kbc_file_storage_backend" {
  name = "${var.backend_prefix}-files-bq-driver"
  project = google_project.service_project.project_id
  location = "US"
  storage_class = "STANDARD"
  force_destroy = true
  public_access_prevention = "enforced"
  versioning {
    enabled = false
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 2
      matches_prefix = ["exp-2/"]
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age            = 15
      matches_prefix = ["exp-15/"]
    }
  }
}

output "file_storage_bucket_id" {
  value = google_storage_bucket.kbc_file_storage_backend.id
}

resource "google_project_iam_member" "prj_service_acc_owner" {
  project = google_project.service_project.project_id
  role    = "roles/owner"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_project_iam_member" "prj_service_acc_objAdm" {
  project = google_project.service_project.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_service_account_key" "key_principal" {
  service_account_id = google_service_account.service_account.name
}

resource "local_file" "key_principal" {
  content  = base64decode(google_service_account_key.key_principal.private_key)
  filename = "principal_key.json"
}

resource "google_service_account_key" "big_query_key" {
  service_account_id = google_service_account.service_account.name
}

resource "local_file" "big_query_key" {
  content  = base64decode(google_service_account_key.big_query_key.private_key)
  filename = "big_query_key.json"
}
