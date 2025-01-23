"""
Kafka Producer Script
- Reads CSV files and sends messages to Kafka topics using multi-threading.
- Configuration is read from an external YAML file.
"""
import random
import threading
import time
import logging
import pandas as pd
from confluent_kafka import Producer
from antispamkafka.utils.logger import Logger
from antispamkafka.utils.utils import load_config

# Setup logging
logger = Logger("KafkaProducer",
                log_file="logs/producer.log", level=logging.INFO).get_logger()


def read_csv(file_path, delimiter=',', chunksize=1000):
    """
    Reads a CSV file in chunks and returns a list of rows.

    Args:
        file_path (str): Path to the CSV file.
        delimiter (str): Delimiter used in the file.
        chunksize (int): Number of rows to read per chunk.

    Returns:
        list: List of rows (nested list).
    """
    try:
        data = pd.read_csv(file_path, delimiter=delimiter,
                           dtype=str, chunksize=chunksize)
        rows = []
        for chunk in data:
            rows.extend(chunk.values.tolist())
        return rows
    except Exception as e:
        logger.error("Error reading CSV file: %s", e)
        raise


def delivery_report(err, msg):
    """
    Callback to report the delivery status of a message.

    Args:
        err (KafkaError): Kafka error if message delivery fails.
        msg (Message): Kafka message object.
    """
    if err is not None:
        logger.error("Delivery failed: %s", err)
    else:
        logger.info("Delivered message: %s at partition %s offset %s",
                    msg.topic(), msg.partition(), msg.offset())


def create_producer(kafka_config):
    """
    Create a Kafka producer instance.

    Args:
        kafka_config (dict): Kafka configuration dictionary.

    Returns:
        Producer: Kafka Producer instance.
    """
    try:
        return Producer({'bootstrap.servers': kafka_config['bootstrap_servers']})
    except Exception as e:
        logger.error("Failed to create Kafka Producer: %s", e)
        raise


def send_messages(producer, topic, rows, retry_config, batch_size=(1, 200)):
    """
    Send messages to Kafka topic in batches.

    Args:
        producer (Producer): Kafka producer instance.
        topic (str): Kafka topic name.
        rows (list): List of rows to be sent as messages.
        retry_config (dict): Retry configurations.
        batch_size (tuple): Min and max batch size for sending messages.
    """
    max_attempts = retry_config.get('max_attempts', 3)
    while rows:
        num_rows = random.randint(batch_size[0], min(len(rows), batch_size[1]))
        selected_rows = rows[:num_rows]
        rows = rows[num_rows:]

        batch_message = "\n".join([",".join(map(str, row)) for row in selected_rows])
        attempt = 0

        while attempt < max_attempts:
            try:
                producer.produce(topic, value=batch_message, callback=delivery_report)
                producer.poll(0)
                break
            except BufferError:
                attempt += 1
                backoff = 2 ** attempt
                logger.warning(
                    "BufferError: Retry attempt %d/%d. Backing off for %d seconds.",
                    attempt, max_attempts, backoff
                )
                time.sleep(backoff)
            except Exception as e:
                logger.error("Unexpected error: %s", e)
                attempt += 1

        if attempt == max_attempts:
            logger.error("Failed to send message to topic %s after %d attempts.",
                         topic, max_attempts)
        else:
            logger.info("Message sent to topic %s successfully.", topic)

        time_to_sleep = random.uniform(1, 5)
        logger.info("Batch sent. Sleeping for %.2f seconds.", time_to_sleep)
        time.sleep(time_to_sleep)


def producer_thread(topic_config, rows, kafka_config, retry_config):
    """
    Kafka producer thread function.

    Args:
        topic_config (dict): Kafka topic configuration with name and handler.
        rows (list): List of rows to send.
        kafka_config (dict): Kafka producer configuration.
        retry_config (dict): Retry configurations.
    """
    topic_name = topic_config['name']
    producer = create_producer(kafka_config)
    logger.info("Starting thread for topic: %s", topic_name)
    send_messages(producer, topic_name, rows, retry_config)
    producer.flush()
    logger.info("Thread for topic %s finished.", topic_name)


def main():
    """
    Main function to read data and start producer threads.
    """
    # Load configuration
    config_file = "config.yaml"
    config = load_config(config_file)

    kafka_config = config.get('kafka_producer', {})
    retry_config = config.get('retry', {})
    topics = config.get('topics', [])

    if not topics:
        logger.error("No Kafka topics found in configuration.")
        return

    # Read CSV file
    # Default CSV file path
    csv_file_path = kafka_config.get('csv_path', 'data/sample.csv')
    delimiter = kafka_config.get('delimiter', '|')  # Default delimiter
    rows = read_csv(csv_file_path, delimiter=delimiter)
    logger.info("Read %d rows from %s.", len(rows), csv_file_path)

    # Process each topic in threads
    threads = []
    for topic_config in topics:
        thread = threading.Thread(
            target=producer_thread,
            args=(topic_config, rows.copy(), kafka_config, retry_config)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    logger.info("All threads finished successfully.")


if __name__ == "__main__":
    main()
