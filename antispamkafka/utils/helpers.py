"""
Module containing helper functions for processing Kafka consumer messages
with retry logic, error handling, and multi-threaded processing.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from antispamkafka.utils.logger import Logger

# Setup logging: Initialize logger for KafkaConsumer
logger = Logger(
    "KafkaConsumer", log_file="logs/consumer.log", level=logging.INFO
).get_logger()


def retry(func, *args, max_attempts=3, **kwargs):
    """
    Retry logic for a given function.

    Used to retry a function call a specified number of times
    in case any exception occurs.

    Args:
        func (Callable): The function to be executed with retry logic.
        max_attempts (int): Maximum number of retry attempts (default: 3).
        *args: Positional arguments to be passed to the function.
        **kwargs: Keyword arguments to be passed to the function.

    Returns:
        Any: The result of the function call, or None if it fails after all attempts.
    """
    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log_retry_metadata(kwargs.get("msg"), attempt + 1, e)
            if attempt == max_attempts - 1:
                logger.error(
                    "Max attempts reached. Function %s failed.", func.__name__
                )
                return None
    return None


def save_to_error_file(msg, topic):
    """
    Save error message details to a file.

    Args:
        msg: Kafka message that caused an error.
        topic (str): Name of the Kafka topic where the error occurred.

    Raises:
        Exception: If writing to the file fails.
    """
    error_file = f"/tmp/kafka_error_message_{topic}.txt"
    try:
        with open(error_file, "w", encoding="utf-8") as f:
            f.write(
                f"Topic: {msg.topic()}\nMessage:\n{msg.value().decode('utf-8')}"
            )
        logger.error("Message saved to %s", error_file)
    except Exception as e:
        logger.error("Failed to save error message: %s", e)


def process_chunk(chunk, topic):
    """
    Process each chunk of the message.

    Args:
        chunk (str): A chunk of the Kafka message.
        topic (str): The Kafka topic name.

    Raises:
        Exception: If an error occurs during chunk processing.
    """
    try:
        logger.info(
            "Processing chunk of size %d bytes from topic '%s'", len(chunk), topic
        )
    except Exception as e:
        logger.error("Error processing chunk in topic '%s': %s", topic, e)
        raise


def process_large_message_in_parallel(msg, chunk_size=1024 * 1024, max_workers=4):
    """
    Process large Kafka messages using multi-threading.

    Args:
        msg: The Kafka message to be processed.
        chunk_size (int): Maximum size of each chunk (default: 1MB).
        max_workers (int): Maximum number of threads
        to use for parallel processing (default: 4).

    Raises:
        Exception: If an error occurs while processing the entire message.
    """
    try:
        message = msg.value().decode("utf-8")
        topic = msg.topic()

        chunks = [
            message[i: i + chunk_size]
            for i in range(0, len(message), chunk_size)
        ]

        logger.info("Total %d chunks generated"
                    " for message in topic '%s'", len(chunks), topic)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_chunk, chunk, topic) for chunk in chunks]
            for future in as_completed(futures):
                future.result()

        logger.info("All chunks processed for message in topic '%s'", topic)
    except Exception as e:
        logger.error("Failed to process large message: %s", e)
        raise


def log_retry_metadata(msg, attempt, exception):
    """
    Log metadata for retry logic.

    Args:
        msg (str): Metadata or context related to the retry attempt.
        attempt (int): The current retry attempt number.
        exception (Exception): The exception encountered during the retry.

    Logs:
        Warning information with details about the retry attempt.
    """
    logger.warning(
        "Retrying... Attempt: %d, Message: %s, Error: %s", attempt, msg, str(exception)
    )
