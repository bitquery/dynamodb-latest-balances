import os
import logging
import signal
import sys
import argparse
from src.config_loader import load_config
from src.kafka_consumer import create_consumer, consume_messages
from src.message_decoder import decode_token_block_message
from src.token_processor import process_token_block
from src.ddb_writer import DDBWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

def main():

    parser = argparse.ArgumentParser(description="Write latest balances in DynamoDb")
    parser.add_argument("--config", default="config/kafka_config.yaml", help="Configuration file path")

    args = parser.parse_args()

    config = load_config(args.config)

    ddb_writer = DDBWriter(
        table_name=config["aws"]["ddb_table_name"],
        region=config["aws"]["region"],
        max_workers=config["workers"],
    )

    consumer = create_consumer(config)

    def message_handler(msg):
        try:
            decoded = decode_token_block_message(msg.value())
            records, block_number = process_token_block(decoded)
            logger.info(f"Processing block {block_number}...")

            if records:
                ddb_writer.write_if_newer(records)
                consumer.commit(msg)
                logger.info(f"Messages written to DynamoDB for block {block_number}")

        except Exception as e:
            logger.exception(f"💥 Processing failed: {e}")

    logger.info("🚀 Starting Kafka consumer...")
    try:
        consume_messages(consumer, config["topic"], message_handler)
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    main()
