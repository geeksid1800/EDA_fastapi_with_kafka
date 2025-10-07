from fastapi import FastAPI
import asyncio
from kafka import KafkaConsumer
import json

#CONSTS
KAFKA_BROKER_URL = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_CONSUMER_ID = 'fastapi_consumer'

stop_polling_event = asyncio.Event()
app = FastAPI()

def deserializer(message_stream):
    if not message_stream:
        return None
    try:
        return json.loads(message_stream)
    except Exception as e:
        print(f"Unable to decode: {message_stream}")
        return None

def create_kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        auto_offset_reset = 'earliest', #if a consumer (or group) loses track of partition's offset, it restarts from beginning of partition
        group_id = 'KAFKA_CONSUMER_ID',
        value_deserializer = deserializer
    )
    return consumer

# #https://huzzefakhan.medium.com/apache-kafka-in-python-d7489b139384
# consumer = create_kafka_consumer()
# for message in consumer:
#     '''
#     message = ConsumerRecord(topic='fastapi-topic', partition=0, offset=11, timestamp=1759845668765, timestamp_type=0, key=None, value={'message': 'm1'}, headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=17, serialized_header_size=-1)
#     type(message) =  <class 'kafka.consumer.fetcher.ConsumerRecord'>
#     '''
#     value = message.value
#     print(value['message'] if value else "")
# #run this using `python ./fastapi_consumer/main.py` and it will print out everything in stdout

async def poll_consumer(consumer: KafkaConsumer):
    try:
        while not stop_polling_event.is_set():
            print('Attempting to poll...')
            records: dict|None = consumer.poll(
                timeout_ms=5000, #wait 5s for any new data if no data in buffer already
                max_records=500
                )
            print(f"{records=}")
            if records:
                for record in records.values():
                    for message_obj in record:
                        message_text = json.loads(message_obj.value).get("message")
                        print(f"Received {message_text=}")
                
            await asyncio.sleep(5) #pause our async code here and finish up any other events happening in the loop thread.
    except Exception as e:
        print(f"Ran into Error: {e}")
    finally:
        print("Closing the consumer")
        consumer.close()

'''
whenever we start a polling process, store it here.
Whenever someone sends a GET reqest to /trigger, check if there's a polling task already in tasklist.
If so, we can ignore the trigger request. Otherwise, we create the polling event.
'''
tasklist = []

@app.get('/trigger')
async def trigger_polling_start():
    if not tasklist:
        stop_polling_event.clear() #makes sure the flag is set to 0 so the poll_consumer fn can run
        new_consumer = create_kafka_consumer()
        task = asyncio.create_task(poll_consumer(consumer=new_consumer))
        tasklist.append(task)
        return {'status':'Initiated a new polling request'}
    
    return {'status':'Kafka polling started...'}


@app.get('/stop-trigger')
async def trigger_polling_stop():
    stop_polling_event.set()
    if tasklist:
        tasklist.pop()

    return {'status': "Kafka polling stopped"}