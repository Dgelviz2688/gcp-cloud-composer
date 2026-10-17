#!/usr/bin/env bash
# Elimina todos los recursos para no generar costes
source "$(dirname "$0")/scripts/env.sh"

# 1. Drenar cualquier job de Dataflow activo
for job in $(gcloud dataflow jobs list --region "$REGION" --status active --project "$PROJECT_ID" --format 'value(JOB_ID)'); do
    gcloud dataflow jobs drain "$job" --region "$REGION" --project "$PROJECT_ID"
done

# 2. Destruir la infraestructura creada con Terraform
terraform -chdir="$ROOT_DIR/terraform" destroy

echo "ℹ️  Composer no borra su bucket interno. Búscalo con: gcloud storage buckets list --project $PROJECT_ID"
