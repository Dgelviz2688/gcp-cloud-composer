#!/usr/bin/env bash
# Lab 0: Preparación del proyecto de Google Cloud
source "$(dirname "$0")/scripts/env.sh"

# 1. Autenticarse en Google Cloud (CLI + credenciales para Terraform y los scripts Python)
gcloud auth login
gcloud auth application-default login

# 2. Configurar el proyecto activo
gcloud config set project "$PROJECT_ID"

# 3. Habilitar las APIs necesarias (Terraform también las declara)
gcloud services enable \
    composer.googleapis.com storage.googleapis.com bigquery.googleapis.com \
    pubsub.googleapis.com dataflow.googleapis.com compute.googleapis.com iam.googleapis.com

# 4. Dependencias Python de los scripts locales (en un entorno virtual)
python3 -m venv "$ROOT_DIR/.venv"
"$ROOT_DIR/.venv/bin/pip" install -r "$ROOT_DIR/requirements.txt"

echo "✅ Proyecto listo. Siguiente paso: cd terraform && terraform init && terraform apply"
