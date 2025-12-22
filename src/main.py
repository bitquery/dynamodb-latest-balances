import os
import logging
import signal
import sys
from typing import Optional
from src.config_loader import load_config
from src.kafka_consumer import create_consumer, consume_messages
from src.message_decoder import decode_token_block_message
from src.token_processor import process_token_block
from src.ddb_writer import DDBWriter

# Optional: EMF for CloudWatch (comment out if not deploying to AWS)
try:
    from aws_embedded_metrics import metric_scope
    USE_EMBEDDED_METRICS = True
except ImportError:
    USE_EMBEDDED_METRICS = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

if USE_EMBEDDED_METRICS:
    @metric_scope
    def get_metrics(metrics):
        metrics.set_namespace("Bitquery/TokenProcessor")
        metrics.put_dimensions({"Service": "TokenProcessor"})
        return metrics

def main():
    config = load_config()

    local_mode = os.getenv("LOCAL_MODE", "false").lower() == "true"
    ddb_writer = DDBWriter(
        table_name=config["aws"]["ddb_table_name"],
        region=config["aws"]["region"],
        local_mode=local_mode
    )

    consumer = create_consumer(config)

    def message_handler(msg):
        try:
            decoded = decode_token_block_message(msg.value())
            records = process_token_block(decoded)

            if USE_EMBEDDED_METRICS:
                metrics = get_metrics()
                metrics.put_metric("MessagesProcessed", 1, "Count")
                metrics.put_metric("RecordsWritten", len(records), "Count")

            if records:
                ddb_writer.write_if_newer(records)
                if USE_EMBEDDED_METRICS:
                    metrics = get_metrics()
                    metrics.put_metric("DynamoDBWrites", len(records), "Count")

        except Exception as e:
            logger.exception(f"💥 Processing failed: {e}")
            if USE_EMBEDDED_METRICS:
                metrics = get_metrics()
                metrics.put_metric("Errors", 1, "Count")

    logger.info("🚀 Starting Kafka consumer...")
    try:
        consume_messages(consumer, config["topic"], message_handler)
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    main()
