import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# 1. Función para limpiar y transformar el mensaje que llega de Pub/Sub
CAMPOS = ('usuario', 'producto', 'accion', 'timestamp')

class ParseMessage(beam.DoFn):
    def process(self, element):
        try:
            # Pub/Sub envía los mensajes en formato de bytes, hay que decodificarlos
            mensaje_dict = json.loads(element.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            # En streaming un mensaje corrupto que lanza excepción se reintenta sin fin: lo descartamos
            logging.warning("Mensaje descartado (JSON inválido): %r", element[:200])
            return

        if not isinstance(mensaje_dict, dict) or not all(c in mensaje_dict for c in CAMPOS):
            logging.warning("Mensaje descartado (faltan campos): %r", mensaje_dict)
            return

        # Solo enviamos a BigQuery las columnas del esquema
        yield {c: mensaje_dict[c] for c in CAMPOS}

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
    # Sin "with": el bloque with espera a que el job termine, y un job streaming nunca termina.
    # Así el proceso que lanza el job (Airflow) queda libre en cuanto Dataflow lo acepta.
    p = beam.Pipeline(options=options)
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

    result = p.run()
    # En local (DirectRunner) sí esperamos, para ver el pipeline funcionando en la terminal
    if options.view_as(StandardOptions).runner in (None, 'DirectRunner'):
        result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    print("Iniciando Job de Dataflow en Streaming...")
    run()