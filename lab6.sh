# Instalar Apache Beam con las dependencias de Google Cloud
# pip install apache-beam[gcp]
pip install apache-beam[gcp] --break-system-packages

# Subimos el archivo a la carpeta 'scripts/' de nuestro Data Lake
# Asegúrate de cambiar 'TU_PROJECT_ID' por tu ID real
gcloud storage cp scripts/beam/streaming_pipeline.py gs://secret-griffin-507921-f0-data-lake/scripts/