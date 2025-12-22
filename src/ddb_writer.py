import boto3
from boto3.dynamodb.conditions import Attr
from typing import List, Dict, Any
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

logger = logging.getLogger(__name__)


class DDBWriter:
    def __init__(
        self,
        table_name: str,
        region: str = "us-east-1",
        max_workers: int = 10
    ):
        # Support local DynamoDB via DYNAMODB_ENDPOINT (e.g., http://localhost:8000)
        endpoint_url = os.getenv("DYNAMODB_ENDPOINT")
        self.dynamodb = boto3.resource(
            "dynamodb",
            region_name=region,
            endpoint_url=endpoint_url
        )
        self.table = self.dynamodb.Table(table_name)
        self.max_workers = max_workers

    def write_if_newer(self, records: List[Dict[str, Any]]) -> None:
        """Write a batch of records in parallel using conditional update_item."""
        if not records:
            return

        succeeded = skipped = failed = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._update_item, rec): rec for rec in records}
            for future in as_completed(futures):
                rec = futures[future]
                address = rec.get("address", "unknown")
                try:
                    result = future.result()
                    if result == "success":
                        succeeded += 1
                    elif result == "skipped":
                        skipped += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"💥 Unhandled error for {address}: {e}")
                    failed += 1

        logger.info(f"✅ Batch result: {succeeded} updated, {skipped} skipped, {failed} failed")

        # Optional: EMF metrics (safe to omit if not in AWS)
        try:
            from aws_embedded_metrics import metric_scope
            @metric_scope
            def emit_metrics(metrics):
                metrics.set_namespace("Bitquery/TokenProcessor")
                metrics.put_dimensions({"Service": "DDBWriter"})
                metrics.put_metric("DDBBatchSize", len(records), "Count")
                metrics.put_metric("DDBWrites", succeeded, "Count")
                metrics.put_metric("DDBSkipped", skipped, "Count")
                metrics.put_metric("DDBFailures", failed, "Count")
            emit_metrics()
        except ImportError:
            pass  # Skip EMF in local dev if package not installed

    def _update_item(self, rec: Dict[str, Any]) -> str:
        address = rec.get("address")
        new_block = rec.get("block_number")

        if not address or not isinstance(new_block, int):
            logger.error(f"❌ Invalid record (missing address or block_number): {rec}")
            return "failed"

        try:
            self.table.update_item(
                Key={"address": address},
                UpdateExpression="SET block_number = :bn, block_timestamp = :ts, balance = :bal",
                ConditionExpression=Attr("block_number").not_exists() | Attr("block_number").lt(new_block),
                ExpressionAttributeValues={
                    ":bn": new_block,
                    ":ts": rec["block_timestamp"],
                    ":bal": rec["balance"]
                }
            )
            return "success"
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            return "skipped"
        except Exception as e:
            logger.error(f"❌ DDB error for {address}: {e}")
            return "failed"