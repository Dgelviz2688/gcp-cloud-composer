from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
import os
import random

REGION = os.environ.get('AIRFLOW_VAR_REGION', 'europe-southwest1')

# 1. Función que simula un control de calidad
def check_data_quality():
    print("🔍 Analizando nulos, duplicados y formatos en BigQuery... (simulado)")
    # Simulamos que el 80% de las veces los datos son buenos y el 20% son malos
    calidad_ok = random.choices([True, False], weights=[80, 20])[0]
    
    if calidad_ok:
        print("✅ Calidad superada. Pasando a transformación.")
        # IMPORTANTE: Se devuelve el task_id de la tarea que debe ejecutarse
        return 'transformar_datos'
    else:
        print("❌ Datos corruptos detectados. Enviando alerta.")
        return 'alerta_error_calidad'

# 2. Definición del DAG
with DAG(
    dag_id='lab4_branching_bq',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['curso', 'lab4']
) as dag:

    start = EmptyOperator(task_id='start')

    # Operador de ramificación
    quality_check = BranchPythonOperator(
        task_id='check_calidad',
        python_callable=check_data_quality
    )

    # Camino A: Alerta (Si los datos son malos)
    alert_error = EmptyOperator(task_id='alerta_error_calidad')

    # Camino B: Transformación SQL en BigQuery (Si los datos son buenos)
    transform_data = BigQueryInsertJobOperator(
        task_id='transformar_datos',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `{{ var.value.project_id }}.curso_dwh.ventas_agrupadas` AS
                SELECT 
                    producto, 
                    SUM(cantidad) as total_unidades, 
                    ROUND(SUM(cantidad * precio), 2) as ingresos_totales
                FROM `{{ var.value.project_id }}.curso_dwh.ventas_raw`
                GROUP BY producto
                ORDER BY ingresos_totales DESC
                """,
                "useLegacySql": False,
            }
        },
        location=REGION
    )

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # 3. Definir las dependencias (El Branching)
    start >> quality_check
    quality_check >> alert_error >> end
    quality_check >> transform_data >> end