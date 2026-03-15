"""Dataset versioning value objects."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class DatasetVersion(BaseModel):
    """The version tag and optional parameters for a publish operation."""

    model_config = ConfigDict(frozen=True)

    version: str
    """User-supplied version string (e.g. '2026-03-09'). Stored as last_updated_data_at."""

    params: dict[str, str] | None = None
    """Optional model parameters recorded alongside the version."""

    mode: Literal["dry", "refresh"] | None = None
    """Publish mode.

    None (default): normal publish — idempotent, skips if version already completed.
    "dry": validate schemas only; no data is written to the warehouse.
    "refresh": overwrite an existing version, ignoring the completed checkpoint.
    """


class VersionRecord(BaseModel):
    """Persisted record of a completed publish (written to dbport.lock)."""

    model_config = ConfigDict(frozen=True)

    version: str
    published_at: datetime
    iceberg_snapshot_id: int | None = None
    iceberg_snapshot_timestamp: datetime | None = None
    params: dict[str, str] | None = None
    rows: int | None = None
    completed: bool = False
