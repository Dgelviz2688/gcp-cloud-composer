# 1. Autenticarse en Google Cloud
gcloud auth login
gcloud auth application-default login

# 2. Configurar el proyecto activo (Diles que cambien 'TU_PROJECT_ID')
gcloud config set project secret-griffin-507921-f0

# 3. Habilitar las APIs necesarias para este laboratorio
gcloud services enable composer.googleapis.com storage.googleapis.com