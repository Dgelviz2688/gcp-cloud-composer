#!/usr/bin/env bash
# Lab 3: Ingesta de GCS a BigQuery (requiere haber generado CSVs en el Lab 2)
source "$(dirname "$0")/scripts/env.sh"

import_dag lab3_gcs_to_bq.py
