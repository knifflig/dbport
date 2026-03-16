"""Tests for adapters.primary.client (DBPort integration)."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

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
                client.schema("CREATE OR REPLACE TABLE inputs.emp (geo VARCHAR, year SMALLINT)")
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
        lock_b = TomlLockAdapter(
            lock_path, model_key="wifor.sector", model_root=".", duckdb_path=""
        )

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
        with (
            patch.dict(os.environ, creds),
            patch("dbport.adapters.primary.client._caller_dir", return_value=tmp_path),
            patch("dbport.adapters.primary.client._find_repo_root", return_value=tmp_path),
        ):
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
        with (
            patch.dict(os.environ, creds),
            patch("dbport.adapters.primary.client._caller_dir", return_value=tmp_path),
        ):
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


class TestDBPortModelRoot:
    def test_explicit_model_root_kwarg(self, tmp_path: Path):
        """When model_root is provided, it is used as caller_dir."""
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        model_dir = tmp_path / "my_model"
        model_dir.mkdir()
        with patch.dict(os.environ, creds):
            client = DBPort(
                agency="wifor",
                dataset_id="emp",
                lock_path=str(tmp_path / "dbport.lock"),
                duckdb_path=str(tmp_path / "dbport.duckdb"),
                model_root=str(model_dir),
            )
            assert client._dataset.model_root == str(model_dir.resolve())
            client.close()


class TestDBPortDuckdbPathRelative:
    def test_duckdb_path_outside_repo_root(self, tmp_path: Path):
        """When duckdb_path is outside repo_root, falls back to absolute string."""
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        # Use a sibling directory so relative_to(repo_root) raises ValueError
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        external = tmp_path / "external"
        external.mkdir()
        db_path = external / "dbport.duckdb"
        with patch.dict(os.environ, creds):
            client = DBPort(
                agency="wifor",
                dataset_id="emp",
                lock_path=str(repo_dir / "dbport.lock"),
                duckdb_path=str(db_path),
            )
            # Should not raise — duckdb_path_rel falls back to absolute
            assert client._dataset.duckdb_path == str(db_path)
            client.close()


class TestDBPortRunMethod:
    def test_run_delegates_to_run_service(self, tmp_path: Path):
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
            mock_svc = MagicMock()
            with patch(
                "dbport.application.services.run.RunService",
                return_value=mock_svc,
            ):
                client.run(version="v1", mode="dry")
            mock_svc.execute.assert_called_once_with(client, version="v1", mode="dry")
            client.close()

    def test_run_hook_property(self, tmp_path: Path):
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
            # Falls back to the default model entrypoint
            assert client.run_hook == "main.py"
            # Set a hook
            client._lock.write_run_hook("sql/main.sql")
            assert client.run_hook == "sql/main.sql"
            client.close()

    def test_configure_input_delegates_to_ingest_service(self, tmp_path: Path):
        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        expected_record = MagicMock()

        with patch.dict(os.environ, creds):
            client = DBPort(
                agency="wifor",
                dataset_id="emp",
                lock_path=str(tmp_path / "dbport.lock"),
                duckdb_path=str(tmp_path / "dbport.duckdb"),
            )
            mock_svc = MagicMock()
            mock_svc.configure.return_value = expected_record
            with patch(
                "dbport.application.services.ingest.IngestService",
                return_value=mock_svc,
            ):
                result = client.configure_input(
                    "wifor.source",
                    filters={"geo": "DE"},
                    version="2026-03-14",
                )

            assert result is expected_record
            declaration = mock_svc.configure.call_args.args[0]
            assert declaration.table_address == "wifor.source"
            assert declaration.filters == {"geo": "DE"}
            assert declaration.version == "2026-03-14"
            client.close()


class TestDBPortConfigOnly:
    """Tests for config_only lightweight mode."""

    def test_config_only_no_credentials_required(self, tmp_path: Path):
        """config_only=True skips credential validation."""
        client = DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "dbport.duckdb"),
            config_only=True,
        )
        client.close()  # should not raise

    def test_config_only_columns_meta_works(self, tmp_path: Path):
        """Column metadata can be set in config_only mode."""
        lock_path = tmp_path / "dbport.lock"
        with DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(lock_path),
            duckdb_path=str(tmp_path / "dbport.duckdb"),
            config_only=True,
        ) as port:
            port.columns.geo.meta(codelist_id="NUTS2024", codelist_kind="hierarchical")

        # Verify it was written to lock
        from dbport.adapters.secondary.lock.toml import TomlLockAdapter

        lock = TomlLockAdapter(lock_path, model_key="wifor.emp")
        entries = lock.read_codelist_entries()
        assert "geo" in entries
        assert entries["geo"].codelist_id == "NUTS2024"
        assert entries["geo"].codelist_kind == "hierarchical"

    def test_config_only_columns_attach_works(self, tmp_path: Path):
        """Column attach can be set in config_only mode."""
        lock_path = tmp_path / "dbport.lock"
        with DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(lock_path),
            duckdb_path=str(tmp_path / "dbport.duckdb"),
            config_only=True,
        ) as port:
            port.columns.geo.attach(table="wifor.cl_nuts2024")

        from dbport.adapters.secondary.lock.toml import TomlLockAdapter

        lock = TomlLockAdapter(lock_path, model_key="wifor.emp")
        entries = lock.read_codelist_entries()
        assert entries["geo"].attach_table == "wifor.cl_nuts2024"

    def test_config_only_schema_raises(self, tmp_path: Path):
        """Data methods raise RuntimeError in config_only mode."""
        import pytest

        with DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "dbport.duckdb"),
            config_only=True,
        ) as port:
            with pytest.raises(RuntimeError, match="config_only"):
                port.schema("CREATE TABLE test (id INT)")

    def test_config_only_load_raises(self, tmp_path: Path):
        """load() raises RuntimeError in config_only mode."""
        import pytest

        with DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "dbport.duckdb"),
            config_only=True,
        ) as port:
            with pytest.raises(RuntimeError, match="config_only"):
                port.load("estat.foo")

    def test_config_only_execute_raises(self, tmp_path: Path):
        """execute() raises RuntimeError in config_only mode."""
        import pytest

        with DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "dbport.duckdb"),
            config_only=True,
        ) as port:
            with pytest.raises(RuntimeError, match="config_only"):
                port.execute("SELECT 1")

    def test_config_only_publish_raises(self, tmp_path: Path):
        """publish() raises RuntimeError in config_only mode."""
        import pytest

        with DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "dbport.duckdb"),
            config_only=True,
        ) as port:
            with pytest.raises(RuntimeError, match="config_only"):
                port.publish(version="v1")

    def test_config_only_run_raises(self, tmp_path: Path):
        """run() raises RuntimeError in config_only mode."""
        import pytest

        with DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "dbport.duckdb"),
            config_only=True,
        ) as port:
            with pytest.raises(RuntimeError, match="config_only"):
                port.run(version="v1")

    def test_config_only_no_duckdb_dir_created(self, tmp_path: Path):
        """config_only=True does not create the data/ directory."""
        data_dir = tmp_path / "data"
        assert not data_dir.exists()
        with DBPort(
            agency="wifor",
            dataset_id="emp",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(data_dir / "emp.duckdb"),
            config_only=True,
        ):
            pass
        assert not data_dir.exists()


class TestDBPortAutoSchema:
    def _make_auto_schema_client(self, tmp_path: Path, mock_catalog=None):
        """Helper to create a DBPort instance for auto-schema tests."""
        import pyarrow as pa

        from dbport.adapters.primary.columns import ColumnRegistry
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
        from dbport.adapters.secondary.lock.toml import TomlLockAdapter
        from dbport.domain.entities.dataset import Dataset

        if mock_catalog is None:
            mock_catalog = MagicMock()
            mock_catalog.table_exists.return_value = True
            mock_catalog.load_arrow_schema.return_value = pa.schema(
                [
                    pa.field("geo", pa.string()),
                    pa.field("year", pa.int16()),
                ]
            )
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
        return client

    def test_auto_detect_schema_refreshes_columns(self, tmp_path: Path):
        """When auto-schema succeeds, columns._refresh() is called."""
        client = self._make_auto_schema_client(tmp_path)
        client._auto_detect_schema()

        schema = client._lock.read_schema()
        assert schema is not None
        assert schema.source == "warehouse"
        assert hasattr(client.columns, "geo")
        assert hasattr(client.columns, "year")
        client._compute.close()

    def test_auto_detect_schema_with_progress_callback(self, tmp_path: Path):
        """Progress callback is called during auto-schema detection."""
        from dbport.infrastructure.progress import progress_callback

        client = self._make_auto_schema_client(tmp_path)
        cb = MagicMock()
        token = progress_callback.set(cb)
        try:
            client._auto_detect_schema()
        finally:
            progress_callback.reset(token)

        cb.started.assert_called_once()
        cb.finished.assert_called_once_with("Schema detected from warehouse")
        client._compute.close()

    def test_auto_detect_schema_no_table_with_callback(self, tmp_path: Path):
        """When auto-schema returns None, callback reports 'No existing warehouse table'."""
        from dbport.infrastructure.progress import progress_callback

        mock_catalog = MagicMock()
        mock_catalog.table_exists.return_value = False
        mock_catalog.load_arrow_schema.return_value = None
        mock_catalog.update_table_properties = MagicMock()
        mock_catalog.get_table_property.return_value = None

        client = self._make_auto_schema_client(tmp_path, mock_catalog=mock_catalog)

        # Patch AutoSchemaService to return None
        with patch("dbport.application.services.auto_schema.AutoSchemaService") as mock_cls:
            mock_cls.return_value.execute.return_value = None
            cb = MagicMock()
            token = progress_callback.set(cb)
            try:
                client._auto_detect_schema()
            finally:
                progress_callback.reset(token)

        cb.finished.assert_called_once_with("No existing warehouse table")
        client._compute.close()

    def test_auto_detect_schema_exception_with_callback(self, tmp_path: Path):
        """When auto-schema raises, callback reports 'Schema detection skipped'."""
        from dbport.infrastructure.progress import progress_callback

        mock_catalog = MagicMock()
        mock_catalog.table_exists.side_effect = RuntimeError("network error")

        client = self._make_auto_schema_client(tmp_path, mock_catalog=mock_catalog)

        with patch("dbport.application.services.auto_schema.AutoSchemaService") as mock_cls:
            mock_cls.return_value.execute.side_effect = RuntimeError("network error")
            cb = MagicMock()
            token = progress_callback.set(cb)
            try:
                client._auto_detect_schema()
            finally:
                progress_callback.reset(token)

        cb.finished.assert_called_once_with("Schema detection skipped")
        client._compute.close()


class TestDBPortSyncLocalState:
    def test_sync_exception_does_not_propagate(self, tmp_path: Path):
        """_sync_output_state swallows exceptions."""
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
        from dbport.adapters.secondary.lock.toml import TomlLockAdapter
        from dbport.domain.entities.dataset import Dataset

        client = DBPort.__new__(DBPort)
        client._compute = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        client._lock = TomlLockAdapter(
            tmp_path / "dbport.lock",
            model_key="wifor.emp",
            model_root=".",
            duckdb_path=str(tmp_path / "test.duckdb"),
        )
        client._catalog = MagicMock()
        client._dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

        with patch("dbport.application.services.sync.SyncService") as mock_cls:
            mock_cls.return_value.sync_output_table.side_effect = RuntimeError("sync failed")
            client._sync_output_state()  # should not raise

        client._compute.close()


class TestDBPortUpdateLastFetched:
    def test_update_last_fetched_with_callback(self, tmp_path: Path):
        """Progress callback is invoked during _update_last_fetched."""
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
        from dbport.adapters.secondary.lock.toml import TomlLockAdapter
        from dbport.domain.entities.dataset import Dataset
        from dbport.infrastructure.progress import progress_callback

        client = DBPort.__new__(DBPort)
        client._compute = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        client._lock = TomlLockAdapter(
            tmp_path / "dbport.lock",
            model_key="wifor.emp",
            model_root=".",
            duckdb_path=str(tmp_path / "test.duckdb"),
        )
        client._catalog = MagicMock()
        client._dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

        with patch("dbport.application.services.fetch.FetchService") as mock_cls:
            mock_cls.return_value.execute.return_value = None
            cb = MagicMock()
            token = progress_callback.set(cb)
            try:
                client._update_last_fetched()
            finally:
                progress_callback.reset(token)

        cb.started.assert_called_once_with("Updating warehouse timestamp")
        cb.finished.assert_called_once()
        client._compute.close()


class TestDBPortLoadInputs:
    """Cover _load_inputs exception handling (client.py lines 440-441)."""

    def test_load_inputs_exception_swallowed(self, tmp_path: Path):
        """_load_inputs swallows exceptions and logs debug message."""
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
        from dbport.adapters.secondary.lock.toml import TomlLockAdapter
        from dbport.domain.entities.dataset import Dataset

        client = DBPort.__new__(DBPort)
        client._compute = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        client._lock = TomlLockAdapter(
            tmp_path / "dbport.lock",
            model_key="wifor.emp",
            model_root=".",
            duckdb_path=str(tmp_path / "test.duckdb"),
        )
        client._catalog = MagicMock()
        client._dataset = Dataset(
            agency="wifor",
            dataset_id="emp",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

        with patch("dbport.application.services.sync.SyncService") as mock_cls:
            mock_cls.return_value.sync_inputs.side_effect = RuntimeError("sync failed")
            client._load_inputs()  # should not raise

        client._compute.close()
