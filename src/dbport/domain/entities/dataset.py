"""Dataset identity value objects."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class DatasetKey(BaseModel):
    """Minimal identity for any dataset in the warehouse."""

    model_config = ConfigDict(frozen=True)

    agency: str
    dataset_id: str

    @property
    def table_address(self) -> str:
        """Canonical `<agency>.<dataset_id>` warehouse address."""
        return f"{self.agency}.{self.dataset_id}"


class Dataset(DatasetKey):
    """Full dataset identity including local working paths."""

    model_config = ConfigDict(frozen=True)

    duckdb_path: str
    """Absolute path to the file-backed DuckDB database."""

    lock_path: str
    """Absolute path to the repo-root dbport.lock TOML file."""

    model_root: str
    """Absolute path to the directory where DBPort was initialised (model root)."""
