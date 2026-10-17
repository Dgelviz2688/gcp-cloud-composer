from airflow.sdk import DAG  # En Airflow 3 las clases de autoría de DAGs viven en airflow.sdk
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# Función en Python que Airflow va a ejecutar
def print_hello():
    print("🚀 ¡Hola Cloud Composer! La infraestructura está viva y funcionando.")
    return "Misión Cumplida"

# Definición del DAG
with DAG(
    dag_id='lab1_hello_world',
    start_date=datetime(2024, 1, 1), # Fecha en el pasado para que esté listo al instante
    schedule=None,                   # Solo se ejecuta manualmente
    catchup=False,                   # Evita que se ejecute por todos los días pasados
    tags=['curso', 'lab1']
) as dag:

    # Definición de las Tareas
    start = EmptyOperator(task_id='start')
    
    hello = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )
    
    end = EmptyOperator(task_id='end')

    # Orquestación (Dependencias)
    start >> hello >> end