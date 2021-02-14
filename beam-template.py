import logging
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions


class PrintTransformation(beam.DoFn):
    """Esta PTransformación imprime los elementos en pantalla"""

    def process(self, element):
        print(element)



def run():

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True


    with beam.Pipeline(options=options) as p:

        records = (p | "Lectura de PubSub" >> beam.io.ReadFromPubSub(None, "projects/gold-braid-297420/subscriptions/pruebaraspi") | "Parseo JSON a Dict" >> beam.Map(json.loads))

        records | "Escritura en BigQuery" >> beam.io.WriteToBigQuery(
            "medidas",
            dataset="prueba",
            project="gold-braid-297420",
            schema="humedad:FLOAT, temperatura:FLOAT, llama:FLOAT, mq7:FLOAT, mq2:FLOAT, tsl:FLOAT, timestamp:TIMESTAMP",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )


        records | beam.ParDo(PrintTransformation())


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()