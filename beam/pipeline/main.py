import logging
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.coders import StrUtf8Coder

import boto3
import psycopg2

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

SCHEMA_REGISTRY_URL = 'http://localhost:8085'
KAFKA_BROKER = 'localhost:9092'
RAW_TOPIC = 'events.raw'
MINIO_BUCKET = 'acme.eu-west-1.stg.data.lake'

KAFKA_CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'beam-group',
    'auto.offset.reset': 'earliest'
}


class DeserializeAndEnrichEvent(beam.DoFn):
    def process(self, element):
        key, value, timestamp, headers = element

        schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        raw_schema_str = schema_registry_client.get_latest_version('events.raw-value').schema.schema_str
        deserializer = AvroDeserializer(schema_registry_client, raw_schema_str)

        logging.info(f"Deserializing value: {value}")

        event = deserializer(value, SerializationContext(RAW_TOPIC, MessageField.VALUE))
        if not event:
            logging.error("Failed to deserialize event")
            return

        header_dict = dict(headers)
        correlation_id = None
        for k, v in header_dict.items():
            if k == 'correlationId':
                correlation_id = v.decode('utf-8')

        enriched_event = {
            'data': event['data'],
            'attributes': event['attributes'],
            'metadata': {
                'correlationId': correlation_id,
                'publishTime': datetime.utcfromtimestamp(timestamp / 1000).isoformat(),
                'schemaId': schema_registry_client.get_latest_version('events.raw-value').schema_id
            }
        }
        yield enriched_event


class WriteEventToMinio(beam.DoFn):
    def process(self, event):
        minio_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
        )

        event_json = json.dumps(event)
        key = f"enriched/{event['metadata']['correlationId']}/{datetime.now().isoformat()}.json"

        minio_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=event_json.encode('utf-8'),
            ContentType='application/json'
        )
        logging.info(f"Saved event to Minio at {key}")
        yield event


class WriteEventToPostgres(beam.DoFn):
    def process(self, event):
        conn = psycopg2.connect(

            dbname='holded',
            user='postgres',
            password='postgres',
            host='localhost',
            port=5432
        )
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO events (
                timestamp_stamp,
                message_id_stamp,
                correlation_id,
                type,
                target_subscription,
                new_context_account_id,
                new_context_user_id,
                data
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event['attributes']['timestampStamp'],
                event['attributes']['messageIdStamp'],
                event['metadata']['correlationId'],

                event['attributes']['type'],
                event['attributes']['targetSubscription'],
                event['attributes']['newContextStamp']['accountId'],
                event['attributes']['newContextStamp']['userId'],

                json.dumps(event['data'])
            )
        )

        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Inserted event into Postgres with correlationId {event['metadata']['correlationId']}")
        yield event

class PrintElement(beam.DoFn):
    def process(self, element):
        logging.info(f"Read element from Kafka: {element}")
        yield element


def run():
    pipeline_options = PipelineOptions(
        streaming=True,
        save_main_session=True,
        runner="DirectRunner"
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config=KAFKA_CONSUMER_CONFIG,
                topics=[RAW_TOPIC],
                with_metadata=True
            )
            | "PrintKafkaMessages" >> beam.ParDo(PrintElement())
            | "DeserializeAndEnrichEvent" >> beam.ParDo(DeserializeAndEnrichEvent())
            | "WriteEventToMinio" >> beam.ParDo(WriteEventToMinio())
            | "WriteEventToPostgres" >> beam.ParDo(WriteEventToPostgres())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()