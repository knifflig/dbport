"""Tests for adapters.primary.client (DBPort integration)."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dbport.adapters.primary.client import DBPort


class TestDBPortInit:
    """DBPort init wires adapters correctly without credentials."""

    def test_context_manager_calls_close(self, tmp_path: Path):
        """__exit__ calls close() without error."""
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        with patch.dict(os.environ, creds):
            client = DBPort(
                agency="wifor",
                dataset_id="emp",
                lock_path=str(tmp_path / "dbport.lock"),
                duckdb_path=str(tmp_path / "dbport.duckdb"),
            )
            client.close()  # should not raise

    def test_duckdb_file_created_on_execute(self, tmp_path: Path):
        """DuckDB file is created lazily on first use."""
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        db_path = tmp_path / "dbport.duckdb"
        with patch.dict(os.environ, creds):
            with DBPort(
                agency="wifor",
                dataset_id="emp",
                lock_path=str(tmp_path / "dbport.lock"),
                duckdb_path=str(db_path),
            ) as client:
                client.execute("SELECT 1")
        assert db_path.exists()

    def test_schema_creates_table_and_updates_lock(self, tmp_path: Path):
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        lock_path = tmp_path / "dbport.lock"
        with patch.dict(os.environ, creds):
            with DBPort(
                agency="wifor",
                dataset_id="emp",
                lock_path=str(lock_path),
                duckdb_path=str(tmp_path / "dbport.duckdb"),
            ) as client:
                client.schema(
                    "CREATE OR REPLACE TABLE inputs.emp (geo VARCHAR, year SMALLINT)"
                )
        assert lock_path.exists()
        from dbport.adapters.secondary.lock.toml import TomlLockAdapter
        lock = TomlLockAdapter(lock_path, model_key="wifor.emp", model_root=".", duckdb_path="")
        schema = lock.read_schema()
        assert schema is not None
        names = [c.name for c in schema.columns]
        assert "geo" in names and "year" in names

    def test_lock_scoped_to_model_key(self, tmp_path: Path):
        """Two DBPort instances share one lock file but don't interfere."""
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        lock_path = tmp_path / "dbport.lock"
        with patch.dict(os.environ, creds):
            with DBPort(
                agency="wifor",
                dataset_id="emp",
                lock_path=str(lock_path),
                duckdb_path=str(tmp_path / "emp.duckdb"),
            ) as client_a:
                client_a.schema("CREATE OR REPLACE TABLE inputs.emp (geo VARCHAR)")

            with DBPort(
                agency="wifor",
                dataset_id="sector",
                lock_path=str(lock_path),
                duckdb_path=str(tmp_path / "sector.duckdb"),
            ) as client_b:
                client_b.schema("CREATE OR REPLACE TABLE inputs.sector (nace VARCHAR)")

        from dbport.adapters.secondary.lock.toml import TomlLockAdapter
        lock_a = TomlLockAdapter(lock_path, model_key="wifor.emp", model_root=".", duckdb_path="")
        lock_b = TomlLockAdapter(lock_path, model_key="wifor.sector", model_root=".", duckdb_path="")

        schema_a = lock_a.read_schema()
        schema_b = lock_b.read_schema()
        assert schema_a is not None and schema_b is not None
        assert [c.name for c in schema_a.columns] == ["geo"]
        assert [c.name for c in schema_b.columns] == ["nace"]

    def test_repo_root_discovery(self, tmp_path: Path):
        """When lock_path is not given, lock is placed at repo root (pyproject.toml location)."""
        from dbport.adapters.primary.client import _find_repo_root
        # The project has a pyproject.toml — discovery should find it
        repo_root = _find_repo_root(Path(__file__).parent)
        assert (repo_root / "pyproject.toml").exists()


class TestFindRepoRoot:
    def test_falls_back_to_start_when_no_pyproject(self, tmp_path: Path):
        """_find_repo_root returns start.resolve() when no pyproject.toml exists above."""
        from dbport.adapters.primary.client import _find_repo_root

        # Create an isolated directory tree with no pyproject.toml.
        # Mount a tmpfs so no ancestor contains one.
        deep = tmp_path / "a" / "b" / "c"
        deep.mkdir(parents=True)

        # Patch Path.exists so that pyproject.toml is never found
        _real_exists = Path.exists

        def _fake_exists(self_path: Path) -> bool:
            if self_path.name == "pyproject.toml":
                return False
            return _real_exists(self_path)

        with patch.object(Path, "exists", _fake_exists):
            result = _find_repo_root(deep)

        assert result == deep.resolve()


class TestDBPortCredentialKwargs:
    def test_explicit_credentials_override_env(self, tmp_path: Path):
        """Credential kwargs are forwarded to WarehouseCreds."""
        with patch.dict(os.environ, {}, clear=False):
            client = DBPort(
                agency="wifor",
                dataset_id="emp",
                catalog_uri="https://custom.example.com",
                catalog_token="custom_token",
                warehouse="custom_wh",
                s3_endpoint="https://s3.example.com",
                s3_access_key="ak",
                s3_secret_key="sk",
                lock_path=str(tmp_path / "dbport.lock"),
                duckdb_path=str(tmp_path / "dbport.duckdb"),
            )
            assert client._creds.catalog_uri == "https://custom.example.com"
            assert client._creds.catalog_token == "custom_token"
            assert client._creds.warehouse == "custom_wh"
            assert client._creds.s3_endpoint == "https://s3.example.com"
            assert client._creds.s3_access_key == "ak"
            assert client._creds.s3_secret_key == "sk"
            client.close()


class TestDBPortAutoLockPath:
    def test_lock_path_none_uses_repo_root(self, tmp_path: Path):
        """When lock_path=None, lock file is placed at _find_repo_root(caller_dir)."""
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        # Patch _caller_dir to return tmp_path so auto-discovery uses tmp_path
        # and _find_repo_root to return tmp_path
        with patch.dict(os.environ, creds), \
             patch("dbport.adapters.primary.client._caller_dir", return_value=tmp_path), \
             patch("dbport.adapters.primary.client._find_repo_root", return_value=tmp_path):
            client = DBPort(
                agency="wifor",
                dataset_id="emp",
                duckdb_path=str(tmp_path / "dbport.duckdb"),
            )
            # lock_path should be repo_root / "dbport.lock"
            assert client._dataset.lock_path == str(tmp_path / "dbport.lock")
            client.close()


class TestDBPortAutoDuckdbPath:
    def test_duckdb_path_none_uses_caller_dir(self, tmp_path: Path):
        """When duckdb_path=None, DuckDB is placed at caller_dir/data/<dataset_id>.duckdb."""
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        with patch.dict(os.environ, creds), \
             patch("dbport.adapters.primary.client._caller_dir", return_value=tmp_path):
            client = DBPort(
                agency="wifor",
                dataset_id="mymodel",
                lock_path=str(tmp_path / "dbport.lock"),
            )
            expected = str(tmp_path / "data" / "mymodel.duckdb")
            assert client._dataset.duckdb_path == expected
            client.close()


class TestDBPortMethods:
    def _make_client(self, tmp_path: Path) -> DBPort:
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        with patch.dict(os.environ, creds):
            return DBPort(
                agency="wifor",
                dataset_id="emp",
                lock_path=str(tmp_path / "dbport.lock"),
                duckdb_path=str(tmp_path / "dbport.duckdb"),
            )

    def test_load_delegates_to_ingest_service(self, tmp_path: Path):
        """client.load() creates IngestService and calls execute()."""
        client = self._make_client(tmp_path)
        mock_svc = MagicMock()
        with patch(
            "dbport.application.services.ingest.IngestService",
            return_value=mock_svc,
        ):
            client.load("estat.foo", filters={"wstatus": "EMP"})
        mock_svc.execute.assert_called_once()
        call_args = mock_svc.execute.call_args
        decl = call_args[0][0]
        assert decl.table_address == "estat.foo"
        assert decl.filters == {"wstatus": "EMP"}
        client.close()

    def test_publish_delegates_to_publish_service(self, tmp_path: Path):
        """client.publish() creates PublishService and calls execute()."""
        client = self._make_client(tmp_path)
        mock_publish = MagicMock()
        with patch(
            "dbport.application.services.publish.PublishService",
            return_value=mock_publish,
        ):
            client.publish(version="v1", params={"wstatus": "EMP"}, mode="dry")
        mock_publish.execute.assert_called_once()
        call_args = mock_publish.execute.call_args
        dv = call_args[0][0]
        assert dv.version == "v1"
        assert dv.params == {"wstatus": "EMP"}
        assert dv.mode == "dry"
        client.close()

    def test_close_suppresses_compute_exception(self, tmp_path: Path):
        """close() suppresses exceptions from compute.close()."""
        client = self._make_client(tmp_path)
        client._compute = MagicMock()
        client._compute.close.side_effect = RuntimeError("boom")
        client.close()  # should not raise


class TestDBPortAutoSchema:
    def test_auto_detect_schema_refreshes_columns(self, tmp_path: Path):
        """When auto-schema succeeds, columns._refresh() is called."""
        import pyarrow as pa
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
        from dbport.adapters.secondary.lock.toml import TomlLockAdapter
        from dbport.adapters.primary.columns import ColumnRegistry
        from dbport.domain.entities.dataset import Dataset

        mock_catalog = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_arrow_schema.return_value = pa.schema([
            pa.field("geo", pa.string()),
            pa.field("year", pa.int16()),
        ])
        # update_table_properties is called by _update_last_fetched
        mock_catalog.update_table_properties = MagicMock()
        mock_catalog.get_table_property.return_value = None

        client = DBPort.__new__(DBPort)
        client._compute = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        client._lock = TomlLockAdapter(
            tmp_path / "dbport.lock",
            model_key="wifor.emp",
            model_root=".",
            duckdb_path=str(tmp_path / "test.duckdb"),
        )
        client._catalog = mock_catalog
        client._dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )
        client.columns = ColumnRegistry(client._lock)

        client._auto_detect_schema()

        # Schema should have been auto-detected and columns refreshed
        schema = client._lock.read_schema()
        assert schema is not None
        assert schema.source == "warehouse"
        assert hasattr(client.columns, "geo")
        assert hasattr(client.columns, "year")
        client._compute.close()
