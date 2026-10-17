terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.2"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Variables
variable "project_id" { type = string }
variable "region" {
  type    = string
  default = "europe-southwest1"
}

data "google_project" "this" {}

# 0. APIs necesarias para todos los laboratorios
resource "google_project_service" "apis" {
  for_each = toset([
    "composer.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
  ])
  service            = each.key
  disable_on_destroy = false
}

# 1. Cuenta de servicio dedicada (mínimo privilegio) para Composer y los workers de Dataflow
resource "google_service_account" "composer" {
  account_id   = "composer-curso-sa"
  display_name = "Service Account de Cloud Composer (curso)"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "composer_roles" {
  for_each = toset([
    "roles/composer.worker",     # Obligatorio para Composer 3
    "roles/storage.objectAdmin", # Leer/escribir en el Data Lake
    "roles/bigquery.dataEditor", # Crear y escribir tablas
    "roles/bigquery.jobUser",    # Lanzar jobs de carga y consultas
    "roles/pubsub.editor",       # Beam crea su propia suscripción al topic
    "roles/dataflow.admin",      # Lanzar jobs de Dataflow desde Airflow
    "roles/dataflow.worker",     # Los workers de Dataflow usan esta misma SA
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.composer.email}"
}

# La SA necesita "actuar como" sí misma para lanzar workers de Dataflow con su identidad
resource "google_service_account_iam_member" "composer_self_user" {
  service_account_id = google_service_account.composer.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer.email}"
}

# El agente de servicio de Composer debe poder gestionar la SA del entorno
resource "google_service_account_iam_member" "composer_agent" {
  service_account_id = google_service_account.composer.name
  role               = "roles/composer.ServiceAgentV2Ext"
  member             = "serviceAccount:service-${data.google_project.this.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

# 2. Data Lake (Bucket de Cloud Storage)
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true # Solo para el curso, permite borrar el bucket aunque tenga archivos

  uniform_bucket_level_access = true
  depends_on                  = [google_project_service.apis]
}

# 3. Entorno de Cloud Composer 3 (Airflow 3)
resource "google_composer_environment" "my_composer" {
  name   = "composer-curso"
  region = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      image_version = "composer-3-airflow-3.1.7"
      # Las variables AIRFLOW_VAR_* se leen en los DAGs como {{ var.value.<nombre> }}
      env_variables = {
        AIRFLOW_VAR_PROJECT_ID  = var.project_id
        AIRFLOW_VAR_REGION      = var.region
        AIRFLOW_VAR_DATAFLOW_SA = google_service_account.composer.email
      }
    }
    node_config {
      service_account = google_service_account.composer.email
    }
  }

  depends_on = [
    google_project_iam_member.composer_roles,
    google_service_account_iam_member.composer_agent,
  ]
}

# 4. Data Warehouse (BigQuery Dataset)
resource "google_bigquery_dataset" "dwh" {
  dataset_id                 = "curso_dwh"
  location                   = var.region
  description                = "Data Warehouse para el curso de Composer"
  delete_contents_on_destroy = true # Para que no dé error al destruir el proyecto al final
  depends_on                 = [google_project_service.apis]
}

# 5. Sistema de Mensajería en Tiempo Real (Pub/Sub)
resource "google_pubsub_topic" "streaming_topic" {
  name       = "ventas-streaming"
  depends_on = [google_project_service.apis]
}

# Outputs
output "airflow_uri" {
  value = google_composer_environment.my_composer.config[0].airflow_uri
}

output "dags_bucket" {
  value = google_composer_environment.my_composer.config[0].dag_gcs_prefix
}

output "data_lake_bucket" {
  value = "gs://${google_storage_bucket.data_lake.name}"
}

output "composer_service_account" {
  value = google_service_account.composer.email
}
