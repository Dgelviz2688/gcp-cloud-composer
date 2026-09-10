import csv
import random
from datetime import datetime
from google.cloud import storage
import sys
import os

import subprocess

def get_default_project():
    try:
        return subprocess.check_output(['gcloud', 'config', 'get-value', 'project']).decode('utf-8').strip()
    except:
        return None

# Verificamos que el usuario pase el Project ID como argumento
if len(sys.argv) < 2:
    project_id = get_default_project()
    if not project_id:
        print("❌ Error: No se pudo detectar el PROJECT_ID de gcloud.")
        print("Uso: python generate_batch.py TU_PROJECT_ID")
        sys.exit(1)
    print(f"ℹ️ Usando PROJECT_ID detectado: {project_id}")
else:
    project_id = sys.argv[1]

bucket_name = f"{project_id}-data-lake"

# Generamos un nombre de archivo único con la fecha y hora
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
file_name = f"ventas_{timestamp}.csv"

# Usar una ruta temporal compatible con Windows y Linux
local_file_path = os.path.join(os.path.dirname(__file__), file_name)

print(f"🛠️ Generando datos de ventas en {file_name}...")

# 1. Crear el CSV con datos aleatorios
try:
    with open(local_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id_venta', 'producto', 'cantidad', 'precio'])
        
        for i in range(1, 101): # 100 ventas simuladas
            producto = random.choice(['Laptop', 'Raton', 'Teclado', 'Monitor'])
            cantidad = random.randint(1, 5)
            precio = round(random.uniform(10.0, 1000.0), 2)
            writer.writerow([i, producto, cantidad, precio])
except Exception as e:
    print(f"❌ Error creando archivo local: {e}")
    sys.exit(1)

# 2. Subir el CSV a Google Cloud Storage
print(f"☁️ Subiendo archivo al Data Lake: gs://{bucket_name}/raw/{file_name}")
try:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    # Lo guardamos dentro de una carpeta lógica llamada "raw/"
    blob = bucket.blob(f"raw/{file_name}")
    blob.upload_from_filename(local_file_path)
    print("✅ ¡Archivo subido con éxito!")
except Exception as e:
    print(f"❌ Error al subir a GCS: {e}")
    if "404" in str(e):
        print("💡 TIP: Verifica que el PROJECT_ID sea correcto y que el bucket exista.")
        print(f"💡 Proyecto actual: {project_id}")
        print(f"💡 Bucket intentado: {bucket_name}")
    sys.exit(1)
finally:
    # Limpiamos el archivo local si existe
    if os.path.exists(local_file_path):
        os.remove(local_file_path)

print("✅ ¡Archivo subido con éxito!")