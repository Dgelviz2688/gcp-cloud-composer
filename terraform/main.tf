provider "google" {
  project = var.project_id
  region  = var.region
}

# Variables
variable "project_id" { type = string }
variable "region" { type = string }
variable "service_account_email" { type = string }

# 1. Data Lake (Bucket de Cloud Storage)
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = "EU"
  force_destroy = true # Solo para el curso, permite borrar el bucket aunque tenga archivos
  
  uniform_bucket_level_access = true
}

# 2. Entorno de Cloud Composer 2
resource "google_composer_environment" "my_composer" {
  name   = "composer-curso"
  region = var.region

  config {
    software_config {
      image_version = "composer-3-airflow-3.1.7" # Versión estable de Airflow
      env_variables = {
        project_id = var.project_id
      }
    }
    node_config {
      service_account = var.service_account_email
    }
  }
}

