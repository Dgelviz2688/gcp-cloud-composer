# Subir el DAG automáticamente al bucket interno de Composer
gcloud composer environments storage dags import \
    --environment composer-curso \
    --location europe-southwest1 \
    --source dags/lab1_hello_world.py