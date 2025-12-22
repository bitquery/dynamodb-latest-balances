from confluent_kafka import Consumer, Message
from typing import Iterator, Callable
import logging
import os

logger = logging.getLogger(__name__)

def create_consumer(config: dict) -> Consumer:
    kafka_conf = config["kafka"]
    conf = {
        "bootstrap.servers": ",".join(kafka_conf["bootstrap_servers"]),
        "security.protocol": kafka_conf["security_protocol"],
        "sasl.mechanism": kafka_conf["sasl_mechanism"],
        "sasl.username": kafka_conf["sasl_username"],
        "sasl.password": kafka_conf["sasl_password"],
        "group.id": kafka_conf["group_id"],
        "auto.offset.reset": kafka_conf.get("auto_offset_reset", "earliest"),
        "enable.auto.commit": kafka_conf.get("enable_auto_commit", False),
    }
    return Consumer(conf)

def consume_messages(consumer: Consumer, topic: str, handler: Callable[[Message], None]) -> None:
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                handler(msg)
            except Exception as e:
                logger.exception(f"Error handling message: {e}")
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
