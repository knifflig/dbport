"""TomlLockAdapter — implements ILockStore using a repo-wide dbport.lock (TOML).

A single dbport.lock lives at the repository root (next to pyproject.toml) and
holds state for every model in the repo.  Each model is stored under a
namespaced section:

    [models."wifor.emp__regional_trends"]
    agency      = "wifor"
    dataset_id  = "emp__regional_trends"
    model_root  = "models/emp_regional_trends"
    duckdb_path = "models/emp_regional_trends/data/emp__regional_trends.duckdb"

    [models."wifor.emp__regional_trends".schema]
    ddl = "..."

    [[models."wifor.emp__regional_trends".schema.columns]]
    ...

    [[models."wifor.emp__regional_trends".inputs]]
    ...

    [[models."wifor.emp__regional_trends".versions]]
    ...

The lock file is safe to commit: it stores schema, inputs, and version
history but never credentials.
"""

from __future__ import annotations

import tomllib
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ....domain.entities.codelist import CodelistEntry
    from ....domain.entities.input import IngestRecord
    from ....domain.entities.schema import DatasetSchema
    from ....domain.entities.version import VersionRecord


# ---------------------------------------------------------------------------
# Minimal TOML serialiser (stdlib-only; handles the lock file schema)
# ---------------------------------------------------------------------------

def _toml_str(v: str) -> str:
    return '"' + v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r") + '"'


def _toml_value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, str):
        return _toml_str(v)
    if isinstance(v, datetime):
        return v.isoformat().replace("+00:00", "Z")
    if isinstance(v, dict):
        pairs = ", ".join(
            f"{k} = {_toml_value(val)}" for k, val in v.items() if val is not None
        )
        return "{" + pairs + "}"
    return _toml_str(str(v))


def _write_section(lines: list[str], header: str, data: dict[str, Any]) -> None:
    lines.append(f"[{header}]")
    for k, v in data.items():
        if v is None:
            continue
        lines.append(f"{k} = {_toml_value(v)}")
    lines.append("")


def _write_array_item(lines: list[str], header: str, data: dict[str, Any]) -> None:
    lines.append(f"[[{header}]]")
    for k, v in data.items():
        if v is None:
            continue
        lines.append(f"{k} = {_toml_value(v)}")
    lines.append("")


def _model_prefix(model_key: str) -> str:
    """Return the TOML section prefix for a model key.

    Keys containing dots must be quoted: models."wifor.emp"
    """
    return f'models."{model_key}"'


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class TomlLockAdapter:
    """Reads and writes dbport.lock as a TOML file.

    A single lock file is shared across all models in the repo.  Each
    instance is scoped to one model via ``model_key`` (= agency.dataset_id).

    Implements: ILockStore
    Dependencies: tomllib (stdlib ≥3.11) for reads; custom writer for writes.
    """

    def __init__(
        self,
        path: Path,
        model_key: str = "",
        model_root: str = ".",
        duckdb_path: str = "",
    ) -> None:
        self._path = path
        self._model_key = model_key
        self._model_root = model_root
        self._duckdb_path = duckdb_path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}
        return tomllib.loads(self._path.read_text(encoding="utf-8"))

    def _model_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Return (creating if needed) the sub-document for this model."""
        if self._model_key:
            return doc.setdefault("models", {}).setdefault(self._model_key, {})
        # Legacy / test mode: no model_key → flat top-level doc
        return doc

    def _ensure_model_header(self, model_data: dict[str, Any]) -> None:
        """Write model identity fields into the model sub-document."""
        if not self._model_key:
            return
        parts = self._model_key.split(".", 1)
        model_data["agency"] = parts[0]
        model_data["dataset_id"] = parts[1] if len(parts) == 2 else parts[0]
        model_data["model_root"] = self._model_root
        model_data["duckdb_path"] = self._duckdb_path

    def _save(self, doc: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []

        # Top-level keys (e.g. default_model) before [models.*] sections
        default_model = doc.get("default_model")
        if default_model is not None:
            lines.append(f"default_model = {_toml_str(default_model)}")
            lines.append("")

        models = doc.get("models", {})
        if models:
            # Multi-model structure
            for model_key, model_data in models.items():
                prefix = _model_prefix(model_key)

                # [models."key"] — model identity
                header_data = {
                    k: v for k, v in model_data.items()
                    if k not in ("schema", "inputs", "versions")
                }
                _write_section(lines, prefix, header_data)

                # [models."key".schema]
                schema = model_data.get("schema")
                if schema:
                    _write_section(lines, f"{prefix}.schema", {"ddl": schema.get("ddl"), "source": schema.get("source", "local")})
                    for col in schema.get("columns", []):
                        _write_array_item(lines, f"{prefix}.schema.columns", col)

                # [[models."key".inputs]]
                for inp in model_data.get("inputs", []):
                    _write_array_item(lines, f"{prefix}.inputs", inp)

                # [[models."key".versions]]
                for ver in model_data.get("versions", []):
                    _write_array_item(lines, f"{prefix}.versions", ver)
        else:
            # Legacy / test mode: flat structure (no model_key)
            schema = doc.get("schema")
            if schema:
                _write_section(lines, "schema", {"ddl": schema.get("ddl"), "source": schema.get("source", "local")})
                for col in schema.get("columns", []):
                    _write_array_item(lines, "schema.columns", col)
            for inp in doc.get("inputs", []):
                _write_array_item(lines, "inputs", inp)
            for ver in doc.get("versions", []):
                _write_array_item(lines, "versions", ver)

        self._path.write_text("\n".join(lines), encoding="utf-8")

    # ------------------------------------------------------------------
    # ILockStore — schema
    # ------------------------------------------------------------------

    def read_schema(self) -> DatasetSchema | None:
        from ....domain.entities.schema import ColumnDef, DatasetSchema, SqlDdl

        doc = self._load()
        m = self._model_doc(doc)
        schema_doc = m.get("schema")
        if not schema_doc or not schema_doc.get("ddl"):
            return None
        ddl = SqlDdl(statement=schema_doc["ddl"])
        columns = tuple(
            ColumnDef(name=c["column_name"], pos=c["column_pos"], sql_type=c.get("sql_type", ""))
            for c in schema_doc.get("columns", [])
        )
        source = schema_doc.get("source", "local")
        return DatasetSchema(ddl=ddl, columns=columns, source=source)

    def write_schema(self, schema: DatasetSchema) -> None:
        doc = self._load()
        m = self._model_doc(doc)
        self._ensure_model_header(m)
        m["schema"] = {
            "ddl": schema.ddl.statement,
            "source": schema.source,
            "columns": [
                {
                    "column_name": c.name,
                    "column_pos": c.pos,
                    "sql_type": c.sql_type,
                    "codelist_id": c.name,
                }
                for c in schema.columns
            ],
        }
        self._save(doc)

    # ------------------------------------------------------------------
    # ILockStore — codelist entries
    # ------------------------------------------------------------------

    def read_codelist_entries(self) -> dict[str, CodelistEntry]:
        from ....domain.entities.codelist import CodelistEntry

        doc = self._load()
        m = self._model_doc(doc)
        entries: dict[str, CodelistEntry] = {}
        for col in m.get("schema", {}).get("columns", []):
            name = col["column_name"]
            entries[name] = CodelistEntry(
                column_name=name,
                column_pos=col.get("column_pos", 0),
                codelist_id=col.get("codelist_id", name),
                codelist_type=col.get("codelist_type"),
                codelist_kind=col.get("codelist_kind"),
                codelist_labels=col.get("codelist_labels"),
                attach_table=col.get("attach_table"),
            )
        return entries

    def write_codelist_entry(self, entry: CodelistEntry) -> None:
        doc = self._load()
        m = self._model_doc(doc)
        self._ensure_model_header(m)
        schema = m.setdefault("schema", {"ddl": "", "columns": []})
        columns: list[dict] = schema.setdefault("columns", [])

        new_data = {
            "column_name": entry.column_name,
            "column_pos": entry.column_pos,
            "sql_type": None,
            "codelist_id": entry.codelist_id,
            "codelist_type": entry.codelist_type,
            "codelist_kind": entry.codelist_kind,
            "codelist_labels": entry.codelist_labels,
            "attach_table": entry.attach_table,
        }

        for i, col in enumerate(columns):
            if col.get("column_name") == entry.column_name:
                new_data["sql_type"] = col.get("sql_type")
                columns[i] = new_data
                self._save(doc)
                return

        columns.append(new_data)
        self._save(doc)

    # ------------------------------------------------------------------
    # ILockStore — ingest records
    # ------------------------------------------------------------------

    def read_ingest_records(self) -> list[IngestRecord]:
        from ....domain.entities.input import IngestRecord

        doc = self._load()
        m = self._model_doc(doc)
        return [
            IngestRecord(
                table_address=r["table_address"],
                last_snapshot_id=r.get("last_snapshot_id"),
                last_snapshot_timestamp_ms=r.get("last_snapshot_timestamp_ms"),
                rows_loaded=r.get("rows_loaded"),
                filters=r.get("filters"),
                version=r.get("version"),
            )
            for r in m.get("inputs", [])
        ]

    def write_ingest_record(self, record: IngestRecord) -> None:
        doc = self._load()
        m = self._model_doc(doc)
        self._ensure_model_header(m)
        inputs: list[dict] = m.setdefault("inputs", [])
        raw = {
            "table_address": record.table_address,
            "last_snapshot_id": record.last_snapshot_id,
            "last_snapshot_timestamp_ms": record.last_snapshot_timestamp_ms,
            "rows_loaded": record.rows_loaded,
            "filters": record.filters,
            "version": record.version,
        }
        for i, r in enumerate(inputs):
            if r["table_address"] == record.table_address:
                inputs[i] = raw
                self._save(doc)
                return
        inputs.append(raw)
        self._save(doc)

    # ------------------------------------------------------------------
    # ILockStore — versions
    # ------------------------------------------------------------------

    def read_versions(self) -> list[VersionRecord]:
        from ....domain.entities.version import VersionRecord

        doc = self._load()
        m = self._model_doc(doc)
        records = []
        for v in m.get("versions", []):
            published_at = v.get("published_at")
            if isinstance(published_at, str):
                published_at = datetime.fromisoformat(published_at)
            snapshot_ts = v.get("iceberg_snapshot_timestamp")
            if isinstance(snapshot_ts, str):
                snapshot_ts = datetime.fromisoformat(snapshot_ts)
            records.append(VersionRecord(
                version=v["version"],
                published_at=published_at,
                iceberg_snapshot_id=v.get("iceberg_snapshot_id"),
                iceberg_snapshot_timestamp=snapshot_ts,
                params=v.get("params"),
                rows=v.get("rows"),
                completed=v.get("completed", False),
            ))
        return records

    def append_version(self, record: VersionRecord) -> None:
        doc = self._load()
        m = self._model_doc(doc)
        self._ensure_model_header(m)
        versions: list[dict] = m.setdefault("versions", [])
        raw = {
            "version": record.version,
            "published_at": record.published_at,
            "iceberg_snapshot_id": record.iceberg_snapshot_id,
            "iceberg_snapshot_timestamp": record.iceberg_snapshot_timestamp,
            "params": record.params,
            "rows": record.rows,
            "completed": record.completed,
        }
        for i, v in enumerate(versions):
            if v.get("version") == record.version:
                versions[i] = raw
                self._save(doc)
                return
        versions.append(raw)
        self._save(doc)

    # ------------------------------------------------------------------
    # ILockStore — run hook
    # ------------------------------------------------------------------

    def read_run_hook(self) -> str | None:
        doc = self._load()
        m = self._model_doc(doc)
        return m.get("run_hook")

    def write_run_hook(self, hook: str) -> None:
        doc = self._load()
        m = self._model_doc(doc)
        self._ensure_model_header(m)
        m["run_hook"] = hook
        self._save(doc)
