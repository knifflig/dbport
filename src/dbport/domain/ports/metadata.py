"""IMetadataStore port — abstract interface for dataset metadata management."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..entities.codelist import CodelistEntry, ColumnCodelist
    from ..entities.dataset import DatasetKey
    from ..entities.input import IngestRecord
    from ..entities.version import DatasetVersion
    from .catalog import ICatalog
    from .compute import ICompute


@runtime_checkable
class IMetadataStore(Protocol):
    """Outbound port for metadata and codelist payload management.

    Secondary adapter: adapters/secondary/metadata/
    """

    def build_metadata_json(
        self,
        key: DatasetKey,
        version: DatasetVersion,
        inputs: list[IngestRecord],
        codelists: ColumnCodelist,
        previous_metadata_json: str | None = None,
        snapshot_id: int | None = None,
    ) -> bytes:
        """Build metadata.json payload in-memory. Returns UTF-8 bytes.

        *snapshot_id* is the Iceberg snapshot where this version was committed;
        it is appended to the ``versions`` list so downstream loaders can pin
        to the exact snapshot without a data scan.
        """
        ...

    def generate_codelist_bytes(
        self,
        codelists: ColumnCodelist,
        compute: ICompute,
        output_table: str,
    ) -> dict[str, bytes]:
        """Generate in-memory CSV bytes for each column. Returns {column_name: bytes}."""
        ...

    def attach_to_table(
        self,
        table_address: str,
        metadata_bytes: bytes,
        codelist_bytes: dict[str, bytes],
        codelist_entries: dict[str, CodelistEntry],
        catalog: ICatalog,
    ) -> None:
        """Embed metadata and codelist payloads in Iceberg table properties."""
        ...
