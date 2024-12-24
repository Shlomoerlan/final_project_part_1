import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
import json
import pandas as pd
from toolz import curry
from app_insert_into_db.service.elastic_service import save_events_for_terror

load_dotenv(verbose=True)

def create_consumer(topic, bootstrap_servers):
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='terror_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

@curry
def insert_batch_to_db(batch):
    df = pd.DataFrame(batch)
    print(f"Received batch with {len(df)} records")
    save_events_for_terror(df)

def consume_from_kafka(topic):
    BOOTSTRAP_SERVERS = os.environ['BOOTSTRAP_SERVERS']
    consumer = create_consumer(topic, BOOTSTRAP_SERVERS)
    try:
        for message in consumer:
            batch = message.value
            insert_batch_to_db(batch)
    except Exception as e:
        print(f"Error while consuming data: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    topic = os.environ['TERROR_DATA_ELASTIC']
    consume_from_kafka(topic)
