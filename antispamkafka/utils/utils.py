"""
This module provides utilities for working with Kafka Consumer configurations.
It includes functionality to load Kafka configuration from a YAML file and map
the configuration fields to valid Kafka Consumer configuration fields.
"""

import yaml


def load_config(config_path: str) -> dict:
    """
    Load Kafka configuration from a YAML file.

    Args:
        config_path (str): The path to the YAML file containing Kafka configuration.

    Returns:
        dict: The loaded YAML configuration as a Python dictionary.
    """
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def map_kafka_config(yaml_config: dict) -> dict:
    """
    Map fields from a YAML configuration to valid Kafka Consumer fields.

    Args:
        yaml_config (dict): The YAML configuration as a Python dictionary.

    Returns:
        dict: A Python dictionary with valid Kafka Consumer configuration fields.
    """

    field_mapping = {
        'bootstrap_servers': 'bootstrap.servers',  # Maps to Kafka bootstrap.servers
        'group_id': 'group.id',  # Maps to Kafka group.id
        'auto_offset_reset': 'auto.offset.reset',  # Maps to Kafka auto.offset.reset
        'fetch_max_bytes': 'fetch.max.bytes',  # Maps to Kafka fetch.max.bytes
        # Maps to Kafka max.partition.fetch.bytes
        'max_partition_fetch_bytes': 'max.partition.fetch.bytes'
    }

    kafka_config = {}  # The resulting Kafka Consumer configuration
    for yaml_field, kafka_field in field_mapping.items():
        if yaml_field in yaml_config:
            kafka_config[kafka_field] = yaml_config[yaml_field]  # Transfer config field
    return kafka_config
