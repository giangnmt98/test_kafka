import os
import time
import json
import redis
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable
from logging.handlers import RotatingFileHandler
from redis import exceptions as redis_exceptions


def setup_redis_connection(pool):
    """Establish a connection to the Redis server.

    Args:
        pool (redis.ConnectionPool): A Redis connection pool.

    Returns:
        redis.Redis: A Redis connection object.
    """
    while True:
        try:
            redis_conn = redis.Redis(connection_pool=pool)
            redis_conn.ping()
            print("============== Success connect Redis ==============")
            return redis_conn
        except redis_exceptions.ConnectionError as err:
            print("Error while connecting to Redis:", err)
            time.sleep(3)
        except ValueError as vle:
            print("ValueError Error!", vle)
            time.sleep(3)


def setup_kafka_consumers(topics, bootstrap_servers):
    """Initialize Kafka consumers for given topics.

    Args:
        topics (list): List of Kafka topics to subscribe to.
        bootstrap_servers (str): Kafka bootstrap servers address.

    Returns:
        list: List of KafkaConsumer objects.
    """
    while True:
        try:
            consumers = [
                KafkaConsumer(
                    topic,
                    bootstrap_servers=bootstrap_servers,
                    auto_offset_reset='earliest'
                ) for topic in topics
            ]
            print("============== Success connect Kafka ==============")
            return consumers
        except (KafkaTimeoutError, NoBrokersAvailable, ValueError) as err:
            print("Kafka connection error:", err)
            time.sleep(3)


def process_online_feature(record, redis_conn):
    """Process records from the 'online_feature' Kafka topic.

    Args:
        record (kafka.consumer.fetcher.ConsumerRecord): Kafka consumer record.
        redis_conn (redis.Redis): Redis connection object.
    """
    result = json.loads(record.value.decode('utf-8'))
    redis_key = result['user_id']
    redis_value = {
        'event_datetime': result['date_time'],
        'last_watched_hashed_item_id_v1': result['last_watched_hashed_item_id_v1'],
        'last_watched_hashed_item_id_v2': result['last_watched_hashed_item_id_v2'],
        'created_datetime': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        redis_conn.set(redis_key, json.dumps(redis_value))
    except redis_exceptions.RedisError as err_message:
        print('Error while setting redis key:', err_message)


def setup_logger(logfile_max_size, data_date):
    """Setup the logger with rotating file handler.

    Args:
        logfile_max_size (int): Maximum size of the log file.
        data_date (str): The date used to create the log folder.

    Returns:
        logging.Logger: Configured logger object.
    """
    logger = logging.getLogger('FetchDataRaw')
    logger.setLevel(logging.INFO)

    if len(logger.handlers) > 0:
        logger.handlers.pop()

    new_folder_path = os.path.join(os.getcwd(), 'logs', data_date)
    os.makedirs(new_folder_path, exist_ok=True)

    app_log_handler = RotatingFileHandler(
        os.path.join(new_folder_path, 'data.csv'),
        maxBytes=logfile_max_size,
        backupCount=10
    )
    logger.addHandler(app_log_handler)

    return logger


def main():
    """Main function to set up Redis, Kafka consumers, and process data."""
    pool = redis.ConnectionPool(host='localhost', port=6379)
    redis_conn = setup_redis_connection(pool)

    list_topics = [
        'movie_watch_history', 'movie_watch_history_ab',
        'vod_watch_history', 'vod_watch_history_ab',
        'online_feature'
    ]
    consumers = setup_kafka_consumers(list_topics, 'localhost:9092')

    logfile_max_size = 1000000
    last_data_date = None

    try:
        while True:
            for consumer in consumers:
                msg = consumer.poll(timeout_ms=60000)  # Adjust the timeout as needed
                if msg is None:
                    continue
                for topic_partition, records in msg.items():
                    for record in records:
                        if record.topic == 'online_feature':
                            process_online_feature(record, redis_conn)
                        else:
                            result = json.loads(record.value.decode('utf-8'))
                            data_date = result['date_time'].split(' ')[0]
                            if last_data_date != data_date:
                                logger = setup_logger(logfile_max_size, data_date)
                                last_data_date = data_date

                            logger.info(','.join(map(str, result.values())))
                            print(','.join(map(str, result.values())))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
