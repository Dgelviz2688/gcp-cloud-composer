from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='lab3_ingesta_bq',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['curso', 'lab3']
) as dag:

    start = EmptyOperator(task_id='start')

    # Operador que mueve datos directamente de Storage a BigQuery
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        # Usamos la variable de Airflow para el nombre del bucket
        bucket='{{ var.value.project_id }}-data-lake',
        # Busca todos los CSV en la carpeta raw
        source_objects=['raw/*.csv'],
        # Destino: proyecto.dataset.tabla
        destination_project_dataset_table='{{ var.value.project_id }}.curso_dwh.ventas_raw',
        
        # Definimos el esquema de la tabla de BigQuery
        schema_fields=[
            {'name': 'id_venta', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'producto', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cantidad', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'precio', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ],
        # Trunca la tabla y la vuelve a llenar con todos los CSVs (Full Load)
        write_disposition='WRITE_TRUNCATE',
        # Saltamos la cabecera del CSV
        skip_leading_rows=1,
        # Formato del archivo
        source_format='CSV'
    )

    end = EmptyOperator(task_id='end')

    start >> load_csv_to_bq >> end