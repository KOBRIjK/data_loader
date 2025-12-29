from typing import Dict, Any

from .base import DatabaseConnector
from .hive import HiveConnector
from .oracle import OracleConnector
from .s3_store import S3Connector
from .vertica import VerticaConnector

_MAP = {
    "oracle": OracleConnector,
    "hive": HiveConnector,
    "vertica": VerticaConnector,
    "s3": S3Connector,
}

def create_connector(config: Dict[str, Any]) -> DatabaseConnector:
    """Создаёт экземпляр коннектора по config['type'] = 'oracle'|'hive'|'vertica'|'s3'."""
    t = config.get("type")
    if not t:
        raise ValueError("config должен содержать ключ 'type'")
    cls = _MAP.get(t.lower())
    if cls is None:
        raise ValueError(f"Unknown connector type: {t}")
    return cls()
