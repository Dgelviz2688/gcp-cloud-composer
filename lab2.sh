gcloud composer environments storage dags import \
    --environment composer-curso \
    --location europe-southwest1 \
    --source dags/lab2_sensor_gcs.py 

python3 scripts/data_generators/generate_batch.py curso-gcp-cloud-composer

gcloud composer environments run composer-curso \
    --location europe-southwest1 \
    variables set -- project_id curso-gcp-cloud-composer