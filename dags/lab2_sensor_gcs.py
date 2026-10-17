from airflow.sdk import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='lab2_gcs_sensor',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['curso', 'lab2']
) as dag:

    start = EmptyOperator(task_id='start')

    # Este Sensor pausa el DAG hasta que detecte archivos que empiecen por "ventas_"
    wait_for_csv = GCSObjectsWithPrefixExistenceSensor(
        task_id='esperar_archivo_ventas',
        # Usamos Jinja Templating para leer la variable AIRFLOW_VAR_PROJECT_ID (definida en Terraform)
        bucket='{{ var.value.project_id }}-data-lake',
        prefix='raw/ventas_',
        poke_interval=30, # Revisa el bucket cada 30 segundos
        timeout=600,      # Si pasan 10 minutos (600s) y no hay archivo, el DAG falla
        mode='poke'
    )

    end = EmptyOperator(task_id='end')

    start >> wait_for_csv >> end