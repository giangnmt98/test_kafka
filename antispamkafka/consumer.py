"""Kafka consumer module with dynamic handler loading and YAML-based configuration."""

import importlib
import logging
import threading
import time

from confluent_kafka import Consumer, KafkaError

from antispamkafka.handlers import process_default
from antispamkafka.utils.helpers import (
    process_large_message_in_parallel,
    retry,
    save_to_error_file,
)
from antispamkafka.utils.logger import Logger
from antispamkafka.utils.utils import load_config, map_kafka_config

# Setup logging
logger = Logger(
    "KafkaConsumer", log_file="logs/consumer.log", level=logging.INFO
).get_logger()


def get_handler_function(handler_name, handler_mapping):
    """
    Fetch the handler function dynamically by consulting the mapping from YAML.
    Fallback to process_default if handler is not found.

    Args:
        handler_name (str): Short name of the handler.
        handler_mapping (dict): Mapping of handler names to full import paths.

    Returns:
        function: The corresponding handler function.
    """
    handler_path = handler_mapping.get(handler_name, None)
    if not handler_path:
        logger.warning(
            "Handler '%s' not found in mapping. Using default handler.", handler_name
        )
        return process_default

    try:
        # Load the handler dynamically using importlib
        module_name, function_name = handler_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        handler = getattr(module, function_name)
        logger.info(
            "Handler '%s' loaded successfully from '%s'", function_name, module_name
        )
        return handler
    except (ImportError, AttributeError) as e:
        logger.warning(
            "Failed to dynamically load handler '%s' "
            "from path '%s': %s. Using default handler.",
            handler_name,
            handler_path,
            e,
        )
        return process_default


def handle_large_message_with_retry(msg, retry_times, chunk_size, max_workers):
    """
    Handle retries for processing large Kafka messages with multi-threading.
    Args:
        msg (Message): The Kafka message to process.
        retry_times (int): Maximum number of retry attempts for processing.
        chunk_size (int): Size of each chunk to process in parallel.
        max_workers (int): Number of worker threads for processing.
    Returns:
        None
    """
    for attempt in range(retry_times):
        try:
            logger.info(
                "Attempt %d/%d to process large message in topic '%s'",
                attempt + 1,
                retry_times,
                msg.topic(),
            )
            process_large_message_in_parallel(msg, chunk_size, max_workers)
            return  # If successful, stop processing
        except Exception as e:
            logger.error(
                "Retry %d/%d failed for large message: %s",
                attempt + 1,
                retry_times,
                e,
            )
            if attempt == retry_times - 1:  # If last retry, save the failed message
                save_to_error_file(msg, msg.topic())
                break


def consume_topic(
    topic, consumer_config, handler_function, retry_config, shutdown_event
):
    """
    Consume messages from a Kafka topic and process them.
    Args:
        topic (str): The Kafka topic to subscribe to.
        consumer_config (dict): Configuration for the Kafka consumer.
        handler_function (function): The function to handle messages.
        retry_config (dict): Retry configuration.
        shutdown_event (threading.Event): Event to signal consumer shutdown.
    Returns:
        None
    """
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    logger.info("Started consumer for topic: %s", topic)
    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(1.0)
            if not msg:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("End of partition reached: %s", msg.topic())
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.error("Unknown topic/partition: %s", msg.error())
                else:
                    raise KafkaError(msg.error())
            else:
                try:
                    # Count the rows in the message
                    message_content = msg.value().decode("utf-8")
                    row_count = len(message_content.split("\n"))
                    logger.info(
                        "Received message with %d rows from topic '%s'",
                        row_count,
                        msg.topic(),
                    )
                except Exception as e:
                    logger.warning("Unable to count rows in message: %s", e)
                # Handle large messages (>10MB) using retry and multi-threading
                message_size = len(msg.value())
                if message_size > 10 * 1024 * 1024:
                    logger.warning(
                        "Large message detected (size: %d). Processing in chunks...",
                        message_size,
                    )
                    max_attempts = retry_config.get("max_attempts", 3)
                    chunk_size = retry_config.get("chunk_size", 1024 * 1024)
                    max_workers = retry_config.get("max_workers", 4)
                    handle_large_message_with_retry(
                        msg, max_attempts, chunk_size, max_workers
                    )
                else:
                    # Process smaller messages with simple retry
                    retry(
                        handler_function,
                        max_attempts=retry_config.get("max_attempts", 3),
                        msg=msg,
                    )
    except Exception as e:
        logger.error("Error consuming topic '%s': %s", topic, e)
    finally:
        consumer.close()
        logger.info("Stopped consumer for topic: %s", topic)


def main():
    """
    Main function to initialize Kafka consumers for multiple topics.
    Loads configuration from a YAML file, creates threads to consume messages
    from each topic, and gracefully shuts down when interrupted.
    Returns:
        None
    """
    # Load configuration from YAML file
    config = load_config("config.yaml")
    kafka_config = config["kafka_consumer"]
    kafka_config = map_kafka_config(kafka_config)
    topics = config["topics"]
    retry_config = config["retry"]
    handler_mapping = config.get("handler_mapping", {})  # Load mapping from YAML

    shutdown_event = threading.Event()
    threads = []

    for topic_config in topics:
        topic_name = topic_config["name"]
        handler_name = topic_config["handler"]

        # Fetch the handler dynamically via YAML mapping
        handler_function = get_handler_function(handler_name, handler_mapping)

        # Spawn a thread for each topic
        thread = threading.Thread(
            target=consume_topic,
            args=(
                topic_name,
                kafka_config,
                handler_function,
                retry_config,
                shutdown_event,
            ),
            name=f"ConsumerThread-{topic_name}",
        )
        threads.append(thread)
        thread.start()

    try:
        # Keep the program running until interrupted
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Stopping consumers...")
        shutdown_event.set()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
