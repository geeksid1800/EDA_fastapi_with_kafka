from fastapi import FastAPI,BackgroundTasks
from kafka.admin import KafkaAdminClient, NewTopic
from fastapi_producer.kafka_producer import produce_kafka_message
from contextlib import asynccontextmanager
from fastapi_producer.produce_schema import ProduceMessage

#CONSTS
KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_ADMIN_CLIENT = 'fastapi-admin-client'

@asynccontextmanager
async def lifespan(app: FastAPI): 
    '''
    create a topic on startup of application
    params:
        @app: FastAPI instance of application
    '''
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER_URL,
        client_id = KAFKA_ADMIN_CLIENT
    )
    #check using the admin client if our desired topic is already created in Kafka
    if not KAFKA_TOPIC in admin_client.list_topics():
        admin_client.create_topics(
            new_topics=[
                NewTopic(name=KAFKA_TOPIC, num_partitions=1,replication_factor=1)
                ]
            )
    
    yield

app = FastAPI(lifespan=lifespan)

@app.post('/produce/message/', tags=["Produce Message"])
async def produce_message(message_request:ProduceMessage, background_tasks: BackgroundTasks):
    background_tasks.add_task(produce_kafka_message, message_request) #args are function and then it's arguments
    return {"message":"Message Received"}