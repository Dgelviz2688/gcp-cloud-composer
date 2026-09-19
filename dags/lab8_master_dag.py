from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id='lab8_master_controller',
    start_date=datetime(2026, 1, 1),
    schedule='@daily', # Se ejecutará cada día automáticamente
    catchup=False,
    tags=['curso', 'lab8', 'master'],
    description='DAG Controlador que gobierna el flujo Batch de la empresa'
)
def master_controller_dag():
    
    start = EmptyOperator(task_id='start')

    # 1. Disparamos el Lab 3 (Ingesta de GCS a BigQuery)
    # En Airflow 3, este operador sigue siendo la mejor práctica para dependencias externas
    trigger_ingesta = TriggerDagRunOperator(
        task_id='trigger_ingesta_batch',
        trigger_dag_id='lab3_ingesta_bq', 
        wait_for_completion=True, # El Master espera a que el Lab 3 termine con éxito
        poke_interval=15
    )

    # 2. Disparamos el Lab 4 (Transformación y Calidad de Datos)
    trigger_transformacion = TriggerDagRunOperator(
        task_id='trigger_transformacion_dwh',
        trigger_dag_id='lab4_branching_bq',
        wait_for_completion=True
    )

    # 3. Notificación final usando el decorador @task (TaskFlow API)
    @task
    def notify_success():
        print("📢 [SISTEMA]: Pipelines finalizados. Enviando reporte a los stakeholders...")
        return "Notificación enviada"

    end = EmptyOperator(task_id='end')

    # 4. Definición del flujo (Sintaxis limpia de Airflow 3)
    start >> trigger_ingesta >> trigger_transformacion >> notify_success() >> end

# Instanciar el DAG
master_controller_dag()