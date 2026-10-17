#!/usr/bin/env bash
# Lab 8: DAG maestro que orquesta los Labs 3 y 4
source "$(dirname "$0")/scripts/env.sh"

import_dag lab8_master_dag.py

# Los DAGs disparados deben estar activos; si están en pausa el maestro esperaría para siempre
airflow_cli dags unpause -- lab3_ingesta_bq
airflow_cli dags unpause -- lab4_branching_bq
