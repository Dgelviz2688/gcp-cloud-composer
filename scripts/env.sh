#!/usr/bin/env bash
# Configuración común que cargan todos los scripts labN.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "$ROOT_DIR/config.env" ]]; then
    echo "❌ No existe config.env. Ejecuta: cp config.env.example config.env  (y edita tu PROJECT_ID)"
    exit 1
fi
source "$ROOT_DIR/config.env"

: "${PROJECT_ID:?Define PROJECT_ID en config.env}"
: "${REGION:?Define REGION en config.env}"
: "${COMPOSER_ENV:?Define COMPOSER_ENV en config.env}"

DATA_LAKE="gs://${PROJECT_ID}-data-lake"

# Sube un DAG a la carpeta dags/ del bucket interno de Composer
import_dag() {
    gcloud composer environments storage dags import \
        --project "$PROJECT_ID" \
        --environment "$COMPOSER_ENV" \
        --location "$REGION" \
        --source "$ROOT_DIR/dags/$1"
}

# Ejecuta un comando de la CLI de Airflow dentro de Composer
airflow_cli() {
    gcloud composer environments run "$COMPOSER_ENV" \
        --project "$PROJECT_ID" \
        --location "$REGION" \
        "$@"
}
