import os
from airflow.sdk import dag
# 1. Nuevos Imports obligatorios
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration

from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

REGION = os.environ.get('AIRFLOW_VAR_REGION', 'europe-southwest1')

@dag(
    dag_id='lab7_dataflow_orchestration',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['curso', 'lab7', 'streaming']
)
def dataflow_streaming_dag():
    start = EmptyOperator(task_id='start')

    # 2. El nuevo operador que sustituye al antiguo
    start_streaming = BeamRunPythonPipelineOperator(
        task_id="start_streaming_job",
        runner="DataflowRunner", # Fundamental indicarle que correrá en Dataflow
        py_file="gs://{{ var.value.project_id }}-data-lake/scripts/streaming_pipeline.py",
        
        # 3. Argumentos que le llegan directamente a tu script de Python (argparse)
        pipeline_options={
            "input_topic": "projects/{{ var.value.project_id }}/topics/ventas-streaming",
            "output_table": "{{ var.value.project_id }}:curso_dwh.eventos_streaming",
            "temp_location": "gs://{{ var.value.project_id }}-data-lake/temp/",
            "region": REGION,
            # Los workers de Dataflow usan la SA dedicada creada en Terraform
            "service_account_email": "{{ var.value.dataflow_sa }}"
        },

        # 4. Configuración nativa del clúster de Dataflow
        dataflow_config=DataflowConfiguration(
            job_name="composer-streaming-job",
            location=REGION,
            # ¡AQUÍ SIGUE ESTANDO EL PARÁMETRO MÁGICO!
            wait_until_finished=False
        )
    )

    end = EmptyOperator(task_id='end')

    start >> start_streaming >> end

# Instanciar el DAG
dataflow_streaming_dag()