#!/usr/bin/env bash
# Lab 5: Simulador de eventos en tiempo real con Pub/Sub
source "$(dirname "$0")/scripts/env.sh"

SUB="ventas-streaming-debug"

# Suscripción temporal para comprobar que los mensajes llegan al topic
gcloud pubsub subscriptions create "$SUB" --topic ventas-streaming --project "$PROJECT_ID"

# Publicamos eventos durante 15 segundos
timeout 15 "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/streaming/publisher.py" "$PROJECT_ID" || true

echo "📥 Leyendo algunos mensajes desde la suscripción:"
gcloud pubsub subscriptions pull "$SUB" --limit 5 --auto-ack --project "$PROJECT_ID"

# Limpieza: borramos la suscripción para que no acumule mensajes
gcloud pubsub subscriptions delete "$SUB" --project "$PROJECT_ID" --quiet
