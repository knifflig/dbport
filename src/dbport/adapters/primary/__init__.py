"""Primary (driving) adapters — the public-facing Python API.

DBPort         : single entrypoint, wires all services and secondary adapters
ColumnRegistry / ColumnConfig : fluent column metadata API (client.columns.<name>)
"""

from .client import DBPort
from .columns import ColumnConfig, ColumnRegistry

__all__ = [
    "DBPort",
    "ColumnRegistry",
    "ColumnConfig",
]
