"""
This module provides logging utilities and message processing handlers
for Kafka consumers.
"""

import logging

from antispamkafka.utils.logger import Logger

# Configure logging
logger = Logger(
    "KafkaConsumer", log_file="logs/consumer.log", level=logging.INFO
).get_logger()


def log_retry_metadata(msg, attempt, error):
    """
    Logs details about a failed retry attempt for a Kafka message.

    Args:
        msg: The Kafka message object.
        attempt: The retry attempt number.
        error: The error object associated with the failure.
    """
    logger.error(
        "Retry %d failed for message. Topic=%s, Partition=%d, Offset=%d.",
        attempt,
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )
    logger.error("Error: %s", error)


def process_serving_data(msg):
    """
    Processes a Kafka message from the serving_data_topic.

    Args:
        msg: The Kafka message to be processed.
    """
    logger.info("%s", "=" * 30)
    logger.info("Processing message from serving_data_topic")
    mess_value = msg.value().decode("utf-8")
    # Uncomment the line below if the message content needs to be displayed
    # logger.info(f"Processing message
    # from serving_data_topic: {msg.value().decode('utf-8')}")
    return mess_value


def process_deduplicate_data(msg):
    """
    Processes a Kafka message from the deduplicate_data_topic.

    Args:
        msg: The Kafka message to be processed.
    """
    logger.info("%s", "=" * 30)
    logger.info("Processing message from deduplicate_data_topic")
    mess_value = msg.value().decode("utf-8")
    # Uncomment the line below if the message content needs to be displayed
    # logger.info(f"Processing message
    # from deduplicate_data_topic: {msg.value().decode('utf-8')}")
    return mess_value


def process_default(msg):
    """
    Handles Kafka messages from unknown topics by logging a warning.

    Args:
        msg: The Kafka message object.
    """
    logger.warning("No handler found for topic: %s.", msg.topic())
