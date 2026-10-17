#!/usr/bin/env bash
# Lab 6: Publicar el pipeline de Apache Beam en el Data Lake
source "$(dirname "$0")/scripts/env.sh"

# Subimos el script a la carpeta 'scripts/' del Data Lake, desde donde lo leerá Airflow
gcloud storage cp "$ROOT_DIR/scripts/beam/streaming_pipeline.py" "$DATA_LAKE/scripts/"

# (Opcional) Prueba local con DirectRunner:
#   python3 -m venv .venv-beam && .venv-beam/bin/pip install -r requirements-beam.txt
