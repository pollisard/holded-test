import logging
import uuid
from fastapi import HTTPException
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import boto3
import json

KAFKA_CONFIG = {'bootstrap.servers': 'localhost:9092'}
SCHEMA_REGISTRY_CONFIG = {'url': 'http://localhost:8085'}
RAW_TOPIC = 'events.raw'
DEAD_LETTER_TOPIC = 'events.dead.letter'
MINIO_BUCKET = 'acme.eu-west-1.stg.data.lake'

producer = Producer(KAFKA_CONFIG)
schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

raw_schema_str = schema_registry_client.get_latest_version('events.raw-value').schema.schema_str
avro_serializer = AvroSerializer(schema_registry_client, raw_schema_str)

minio_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} partition [{msg.partition()}]')

def normalize_event(event):
    headers = event.get("headers", {})

    attributes = {
        "type": headers.get("type"),
        "targetSubscription": headers.get("targetSubscription"),
        "newContextStamp": headers.get("X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\NewContextStamp"),
        "timestampStamp": headers.get("X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\TimestampStamp", {}).get("dispatchedAt"),
        "messageIdStamp": headers.get("X-Message-Stamp-Holded\\Core\\Messaging\\Messenger\\Stamp\\MessageIdStamp", {}).get("id"),
        "amplitudeTrackStamp": headers.get("X-Message-Stamp-Holded\\Shared\\Infrastructure\\Messenger\\Stamp\\AmplitudeTrackStamp")
    }

    normalized_event = {
        "data": event.get("data"),
        "attributes": attributes
    }

    return normalized_event

def collect(events):
    correlation_id = str(uuid.uuid4())
    logging.info(f'Collecting events with correlation ID: {correlation_id}')

    for event in events:
        try:
            normalized_event = normalize_event(event)
            serialized_event = avro_serializer(normalized_event, SerializationContext(RAW_TOPIC, MessageField.VALUE))
            producer.produce(
                topic=RAW_TOPIC,
                value=serialized_event,
                headers={"correlationId": correlation_id},
                callback=delivery_report
            )
        except Exception as e:
            logging.error(f'Invalid event: {e}')

            producer.produce(
                topic=DEAD_LETTER_TOPIC,
                value=json.dumps(event).encode('utf-8'),
                headers={'error': str(e), 'correlationId': correlation_id},
                callback=delivery_report
            )

            minio_client.put_object(
                Bucket=MINIO_BUCKET,
                Key=f'dead_letter/{correlation_id}/{uuid.uuid4()}.json',
                Body=json.dumps(event).encode('utf-8'),
                ContentType='application/json'
            )

    producer.flush()

    return {'status': 'OK', 'correlation_id': correlation_id}