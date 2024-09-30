"""
This is the main module for the Kafka producer, reading data from parquet files,
and sending it to Kafka topics using multi-threading.
"""

import os
import json
import time
import random
import threading
import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable


def load_data(folder_path='data'):
    """
    Load data from parquet files in the given folder and store in a dictionary.

    Args:
        folder_path (str): Path to the folder containing parquet files.

    Returns:
        tuple: List of topics and dictionary of data loaded from parquet files.
    """
    list_topic = []
    data_dict = {}
    for file_name in os.listdir(folder_path):
        if '.parquet' in file_name and file_name not in ['user_embedding.parquet', 'tag_definitions.parquet']:
            list_topic.append(file_name.split('.')[0])
            try:
                data = pd.read_parquet(os.path.join(folder_path, file_name))
                data['source'] = file_name.split('.')[0]
                data = data[['source', 'date_time', 'content_id', 'content_type', 'profile_id', 'duration']]
                data_dict[file_name.split('.')[0]] = data.to_json(orient='records')
                print(f"Loaded data from {file_name} into {file_name} variable.")
            except (FileNotFoundError, pd.errors.EmptyDataError):
                print(f"Error loading file {file_name}.")
    return list_topic, data_dict


def thread_worker(kafka_producer, json_str, topic_name, index):
    """
    Worker function for threading to process and send messages to Kafka topics.

    Args:
        kafka_producer (KafkaProducer): Kafka producer instance.
        json_str (str): JSON string of data to be sent.
        topic_name (str): Kafka topic name.
        index (int): Thread index.
    """
    json_list = json.loads(json_str)
    for item in json_list:
        print(f"Thread-{index} processed topic: {topic_name}")
        kafka_producer.send(topic_name, item)
        time.sleep(random.randint(1, 3))


def send_messages(kafka_producer, list_topic, data_dict):
    """
    Create and start threads to send messages to Kafka.

    Args:
        kafka_producer (KafkaProducer): Kafka producer instance.
        list_topic (list): List of Kafka topics.
        data_dict (dict): Dictionary containing data to be sent.
    """
    threads = [
        threading.Thread(
            target=thread_worker,
            args=(kafka_producer, data_dict.get(list(data_dict.keys())[i]), list_topic[i], i)
        ) for i in range(len(list_topic))
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("All threads have finished.")


def serializer(message):
    """
    Serializes a message to JSON and encodes it to UTF-8.

    Args:
        message (dict): The message to be serialized.

    Returns:
        bytes: Serialized JSON message.
    """
    return json.dumps(message).encode('utf-8')


def create_kafka_client():
    """
    Creates and returns a Kafka producer and admin client.

    Returns:
        tuple: Kafka producer and admin client.
    """
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=serializer
            )
            admin_client = KafkaAdminClient(
                bootstrap_servers='localhost:9092'
            )
            print("Successfully connected to Kafka at localhost:9092")
            return producer, admin_client
        except (KafkaTimeoutError, NoBrokersAvailable, ValueError) as e:
            print(f"Kafka connection error: {e}")
            time.sleep(3)


if __name__ == '__main__':
    list_topic, data_dict = load_data()
    kafka_producer, admin_client = create_kafka_client()

    if 'online_feature' not in admin_client.list_topics():
        topic_list = [NewTopic(name="online_feature", num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    send_messages(kafka_producer=kafka_producer, list_topic=list_topic, data_dict=data_dict)
