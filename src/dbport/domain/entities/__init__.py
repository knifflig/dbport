"""Domain entities — immutable value objects modelled with Pydantic.

All models use ConfigDict(frozen=True) to enforce immutability.
"""

from .codelist import CodelistEntry, ColumnCodelist
from .dataset import Dataset, DatasetKey
from .input import IngestRecord, InputDeclaration
from .schema import ColumnDef, DatasetSchema, SqlDdl
from .version import DatasetVersion, VersionRecord

__all__ = [
    "Dataset",
    "DatasetKey",
    "ColumnDef",
    "DatasetSchema",
    "SqlDdl",
    "InputDeclaration",
    "IngestRecord",
    "DatasetVersion",
    "VersionRecord",
    "CodelistEntry",
    "ColumnCodelist",
]
