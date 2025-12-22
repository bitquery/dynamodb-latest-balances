import yaml
from pathlib import Path
from typing import Dict, Any

def load_config(config_path: str = "config/kafka_config.yaml") -> Dict[str, Any]:
    with open(Path(config_path), "r") as f:
        return yaml.safe_load(f)
