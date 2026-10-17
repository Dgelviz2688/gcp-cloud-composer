#!/usr/bin/env bash
# Lab 2: Sensor de GCS - el DAG espera a que llegue un archivo al Data Lake
source "$(dirname "$0")/scripts/env.sh"

import_dag lab2_sensor_gcs.py
echo "👉 Lanza 'lab2_gcs_sensor' desde la UI de Airflow y pulsa ENTER para generar el archivo..."
read -r

"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/data_generators/generate_batch.py" "$PROJECT_ID"
