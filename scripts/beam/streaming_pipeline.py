import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# 1. Función para limpiar y transformar el mensaje que llega de Pub/Sub
class ParseMessage(beam.DoFn):
    def process(self, element):
        # Pub/Sub envía los mensajes en formato de bytes, hay que decodificarlos
        mensaje_texto = element.decode('utf-8')
        # Convertimos el texto JSON a un diccionario de Python
        mensaje_dict = json.loads(mensaje_texto)
        
        # Opcional: Aquí podrías añadir lógica de limpieza (ej. validar si el usuario existe)
        
        yield mensaje_dict

def run(argv=None):
    # 2. Definimos los argumentos que recibirá el script (se los pasará Airflow en el Lab 7)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True, help='Topic de Pub/Sub de entrada')
    parser.add_argument('--output_table', required=True, help='Tabla de BigQuery de destino')
    
    known_args, pipeline_args = parser.parse_known_args(argv)

    # 3. Configuramos las opciones del pipeline indicando que es modo STREAMING
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    # 4. Construcción del Grafo (Pipeline) de Apache Beam
    with beam.Pipeline(options=options) as p:
        (
            p 
            # Paso A: Leer de Pub/Sub
            | 'LeerDesdePubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            
            # Paso B: Parsear el JSON
            | 'ParsearJSON' >> beam.ParDo(ParseMessage())
            
            # Paso C: Escribir en BigQuery en tiempo real
            | 'EscribirEnBigQuery' >> beam.io.WriteToBigQuery(
                known_args.output_table,
                # Definimos el esquema para que Dataflow cree la tabla si no existe
                schema='usuario:STRING, producto:STRING, accion:STRING, timestamp:INTEGER',
                # WRITE_APPEND significa que añadiremos filas al final de la tabla sin borrar lo anterior
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                # CREATE_IF_NEEDED creará la tabla automáticamente la primera vez
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    print("Iniciando Job de Dataflow en Streaming...")
    run()