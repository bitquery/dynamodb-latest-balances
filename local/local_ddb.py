import json
import os
from typing import Dict, Any

class LocalDynamoDBTable:
    def __init__(self, table_name: str):
        self.file_path = f".local_ddb_{table_name}.json"
        self._data = self._load()

    def _load(self) -> Dict[str, Any]:
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as f:
                return json.load(f)
        return {}

    def _save(self):
        with open(self.file_path, "w") as f:
            json.dump(self._data, f, indent=2)

    def put_item(self, Item: Dict[str, Any], ConditionExpression=None):
        key = Item["address"]
        existing = self._data.get(key)

        should_write = True
        if ConditionExpression:
            if existing and "block_number" in existing:
                if not ConditionExpression(existing, Item):
                    raise ConditionalCheckFailedException()

        self._data[key] = Item
        self._save()

class ConditionalCheckFailedException(Exception):
    pass

# Minimal Attr evaluator for local mode
class Attr:
    def __init__(self, name):
        self.name = name

    def not_exists(self):
        return lambda existing, _: self.name not in existing

    def lt(self, value):
        return lambda existing, _: existing.get(self.name, -1) < value

    def __or__(self, other):
        return lambda existing, new: self(existing, new) or other(existing, new)

# Patch boto3.dynamodb.conditions.Attr in local mode
import sys
sys.modules["boto3.dynamodb.conditions"] = type("mock", (), {"Attr": Attr})()
