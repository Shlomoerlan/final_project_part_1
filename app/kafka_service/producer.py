import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from toolz import curry, partition_all
from app.service.csv_service_psql import merged_df

load_dotenv(verbose=True)

@curry
def serialize_to_json(data):
    return json.dumps(data).encode('utf-8')


def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=serialize_to_json
    )


@curry
def send_batch(producer, topic, batch):
    producer.send(topic, batch)
    print(f"Sent batch with {len(batch)} records")


def send_to_kafka(df, topic, batch_size=100):
    producer = create_producer(os.environ['BOOTSTRAP_SERVERS'])
    try:
        batches = partition_all(batch_size, df.to_dict(orient='records'))
        for batch in batches:
            send_batch(producer, topic, list(batch))

        print("All data sent successfully!")
    except Exception as e:
        print(f"Error while sending data: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    topic = os.environ['TERROR_DATA_PSQL']
    send_to_kafka(merged_df, topic)
