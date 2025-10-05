from kafka import KafkaProducer
from fastapi import HTTPException
from produce_schema import ProduceMessage
import json

#CONSTS
KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
PRODUCER_CLIENT_ID = 'fastapi_producer'

#Serialise the information before sending the message to the topic
def serializer(message):
    return json.dumps(message).encode() #converts a dict to a JSON string and encode; default encoding is UTF-8

producer = KafkaProducer(
    api_version = (3,9), #latest stable version, has all feautures
    bootstrap_servers = KAFKA_BROKER_URL,
    serializer = serializer,
    client_id = PRODUCER_CLIENT_ID
)

def produce_kafka_message(message: ProduceMessage):
    try:
        producer.send(KAFKA_TOPIC, json.dumps({'message':message}))
        producer.flush() #ensures all messages are sent
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail='Failed to produce message')