"""DBPort — DuckDB-native runtime for building reproducible warehouse datasets.

Single public import:

    from dbport import DBPort
"""

from __future__ import annotations

from .adapters.primary.client import DBPort

__all__ = ["DBPort"]
