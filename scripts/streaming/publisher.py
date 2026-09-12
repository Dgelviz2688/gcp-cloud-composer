import json
import time
import random
import sys
from google.cloud import pubsub_v1

if len(sys.argv) < 2:
    print("❌ Error: Debes proporcionar tu PROJECT_ID.")
    print("Uso: python publisher.py TU_PROJECT_ID")
    sys.exit(1)

project_id = sys.argv[1]
topic_id = "ventas-streaming"

# Iniciamos el cliente de Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

print("🚀 Iniciando el simulador de e-commerce en Streaming...")
print(f"📡 Emitiendo eventos al topic: {topic_path}")
print("🛑 Presiona Ctrl+C para detener el streaming.")
print("-" * 50)

# Lista de productos de nuestro e-commerce ficticio
productos =["Laptop Pro", "Raton Inalambrico", "Teclado Mecanico", "Monitor 4K", "Auriculares Bluetooth"]
acciones = ["view_item", "add_to_cart", "purchase"]

try:
    # Bucle infinito: simula el tráfico continuo de una web
    while True:
        # 1. Generamos un evento aleatorio
        evento = {
            "usuario": f"user_{random.randint(1000, 9999)}",
            "producto": random.choice(productos),
            "accion": random.choices(acciones, weights=[70, 20, 10])[0], # 70% vistas, 20% carritos, 10% compras
            "timestamp": int(time.time())
        }
        
        # 2. Convertimos el diccionario a un string JSON codificado en bytes
        data_str = json.dumps(evento).encode("utf-8")
        
        # 3. Publicamos el mensaje en Pub/Sub
        future = publisher.publish(topic_path, data_str)
        
        # Mostramos en pantalla el evento y su ID único generado por Google Cloud
        print(f"✅ Evento publicado: {evento} | ID de mensaje: {future.result()}")
        
        # Pausa aleatoria entre 0.5 y 2 segundos para simular tráfico orgánico
        time.sleep(random.uniform(0.5, 2.0))

except KeyboardInterrupt:
    print("\n🛑 Streaming detenido por el usuario.")