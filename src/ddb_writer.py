import boto3
from boto3.dynamodb.conditions import Attr
from typing import List, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)

class DDBWriter:
    def __init__(self, table_name: str, region: str = "us-east-1", local_mode: bool = False):
        self.table_name = table_name
        self.local_mode = local_mode

        if local_mode:
            from local.local_ddb import LocalDynamoDBTable
            self.table = LocalDynamoDBTable(table_name)
        else:
            endpoint = os.getenv("DYNAMODB_ENDPOINT")
            dynamodb = boto3.resource(
                "dynamodb",
                region_name=region,
                endpoint_url=endpoint if endpoint else None
            )
            self.table = dynamodb.Table(table_name)

    def write_if_newer(self, records: List[Dict[str, Any]]) -> None:
        for rec in records:
            key = {"address": rec["address"]}
            new_block = rec["block_number"]
            try:
                self.table.put_item(
                    Item=rec,
                    ConditionExpression=Attr("block_number").not_exists() | (Attr("block_number").lt(new_block))
                )
                logger.debug(f"✅ Wrote {rec['address']} @ block {new_block}")
            except self.table.meta.client.exceptions.ConditionalCheckFailedException:
                logger.debug(f"⏭️  Skipped {rec['address']}: older or equal block {new_block}")
            except Exception as e:
                logger.error(f"❌ Failed to write {rec['address']}: {e}")
