terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  # Configuration options
}

variable "organization_id" {
  type = string
}

variable "backend_folder_display_name" {
  type = string
  default = "Keboola Storage Backend"
}

variable "service_project_name" {
  type = string
  default = "Service project"
}

variable "service_project_id" {
  type = string
  default = "backend-service-project"
}

variable "service_account_id" {
  type = string
  default = "backend-service-acc"
}

variable "service_account_name" {
  type = string
  default = "Backend Service Acc"
}

resource "google_folder" "storage_backend_folder" {
  display_name = var.backend_folder_display_name
  parent       = "organizations/${var.organization_id}"
}

resource "google_project" "service_project_in_a_folder" {
  name       = var.service_project_name
  project_id = var.service_project_id
  folder_id  = google_folder.storage_backend_folder.id
}

resource "google_service_account" "service_account" {
  account_id = var.service_account_id
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

resource "google_project_service" "enable_cloud_resource_manager_api" {
  service                    = "cloudresourcemanager.googleapis.com"
  project = google_project.service_project_in_a_folder.project_id
  disable_dependent_services = true
}

output "folder_id" {
  value = google_folder.storage_backend_folder.id
}

output "service_project_id" {
  value = google_project.service_project_in_a_folder.project_id
}
