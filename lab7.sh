#!/usr/bin/env bash
# Lab 7: Orquestar un job de Dataflow en streaming desde Airflow (requiere el Lab 6)
source "$(dirname "$0")/scripts/env.sh"

import_dag lab7_dataflow_streaming.py

cat <<MSG
✅ DAG subido. Pasos siguientes:
  1. Lanza 'lab7_dataflow_orchestration' en la UI de Airflow.
  2. Espera a que el job aparezca en estado "Running" en la consola de Dataflow (~3-5 min).
  3. Genera tráfico:  .venv/bin/python scripts/streaming/publisher.py $PROJECT_ID
  4. ⚠️  Detén el job al terminar (un job streaming cobra mientras está activo):
       gcloud dataflow jobs list --region $REGION --status active
       gcloud dataflow jobs drain JOB_ID --region $REGION
MSG
