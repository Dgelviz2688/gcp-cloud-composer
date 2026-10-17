#!/usr/bin/env bash
# Lab 1: Hello World - primer DAG en Cloud Composer
source "$(dirname "$0")/scripts/env.sh"

import_dag lab1_hello_world.py
echo "✅ DAG subido. En 1-2 minutos aparecerá 'lab1_hello_world' en la UI de Airflow."
