terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.43.0"
    }
  }
}

provider "google" {
  # Configuration options
}

variable "organization_id" {
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
    "cloudbilling.googleapis.com"
  ]
}

resource "google_folder" "storage_backend_folder" {
  display_name = local.backend_folder_display_name
  parent       = "organizations/${var.organization_id}"
}

resource "google_project" "service_project_in_a_folder" {
  name       = local.service_project_name
  project_id = local.service_project_id
  folder_id  = google_folder.storage_backend_folder.id
  billing_account = var.billing_account_id
}

resource "google_project_service" "services" {
  for_each = toset(var.services)
  project                    = google_project.service_project_in_a_folder.project_id
  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
  depends_on = [google_project.service_project_in_a_folder]
}

resource "google_service_account" "service_account" {
  account_id = local.service_account_id
  description = "Service account to managing keboola backend projects"
  project = google_project.service_project_in_a_folder.project_id
}

resource "google_folder_iam_binding" "folder_service_acc_project_creator_role" {
  folder  = google_folder.storage_backend_folder.name
  role    = "roles/resourcemanager.projectCreator"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_folder_iam_binding" "folder_service_acc_project_list_role" {
  folder  = google_folder.storage_backend_folder.name
  role    = "roles/browser"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

output "folder_id" {
  value = google_folder.storage_backend_folder.id
}

output "service_project_id" {
  value = google_project.service_project_in_a_folder.project_id
}

resource "google_storage_bucket" "kbc_file_storage_backend" {
  name = "${var.backend_prefix}-files-bq-driver"
  project = google_project.service_project_in_a_folder.project_id
  location = "US"
  storage_class = "STANDARD"
  force_destroy = true
  public_access_prevention = "enforced"
  versioning {
    enabled = false
  }
}

output "file_storage_bucket_id" {
  value = google_storage_bucket.kbc_file_storage_backend.id
}

resource "google_project_iam_binding" "prj_service_acc_owner" {
  project = google_project.service_project_in_a_folder.project_id
  role = "roles/owner"
  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_project_iam_binding" "prj_service_acc_objAdm" {
  project = google_project.service_project_in_a_folder.project_id
  role = "roles/storage.objectAdmin"
  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}