"""Contract tests — lock the public DBPort surface for 0.1.0.

These tests exist to prevent accidental changes to the public API.
Any change that breaks a test here is a signal that the public contract
is shifting and must be reviewed deliberately.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Module-level exports
# ---------------------------------------------------------------------------


class TestModuleExports:
    def test_all_contains_only_dbport(self):
        import dbport

        assert dbport.__all__ == ["DBPort"]

    def test_dbport_importable(self):
        from dbport import DBPort  # noqa: F401


# ---------------------------------------------------------------------------
# Constructor signature
# ---------------------------------------------------------------------------


class TestConstructorSignature:
    def test_positional_params(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.__init__)
        params = list(sig.parameters.values())
        # Skip 'self'
        positional = [
            p for p in params if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD and p.name != "self"
        ]
        assert [p.name for p in positional] == ["agency", "dataset_id"]
        for p in positional:
            assert p.default is inspect.Parameter.empty, f"{p.name} should be required"

    def test_keyword_only_params(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.__init__)
        kw_only = {
            name: p.default
            for name, p in sig.parameters.items()
            if p.kind == inspect.Parameter.KEYWORD_ONLY
        }

        expected = {
            "catalog_uri": None,
            "catalog_token": None,
            "warehouse": None,
            "s3_endpoint": None,
            "s3_access_key": None,
            "s3_secret_key": None,
            "duckdb_path": None,
            "lock_path": None,
            "model_root": None,
            "load_inputs_on_init": True,
            "config_only": False,
        }
        assert kw_only == expected

    def test_total_parameter_count(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.__init__)
        # 13 params + self = 14
        assert len(sig.parameters) == 14


# ---------------------------------------------------------------------------
# Public method and property inventory
# ---------------------------------------------------------------------------


class TestPublicSurface:
    def test_public_methods(self):
        from dbport import DBPort

        expected_methods = {
            "schema",
            "load",
            "execute",
            "publish",
            "close",
            "configure_input",
            "run",
        }
        actual = {
            name
            for name in dir(DBPort)
            if not name.startswith("_") and callable(getattr(DBPort, name))
        }
        assert actual == expected_methods

    def test_public_properties(self):
        from dbport import DBPort

        expected_properties = {"run_hook"}
        actual = {
            name
            for name in dir(DBPort)
            if not name.startswith("_") and isinstance(getattr(DBPort, name), property)
        }
        assert actual == expected_properties

    def test_context_manager_protocol(self):
        from dbport import DBPort

        assert hasattr(DBPort, "__enter__")
        assert hasattr(DBPort, "__exit__")

    def test_columns_attribute_set_on_init(self, tmp_path: Path):
        from dbport import DBPort

        client = DBPort(
            agency="test",
            dataset_id="ds",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "test.duckdb"),
            config_only=True,
        )
        assert hasattr(client, "columns")


# ---------------------------------------------------------------------------
# Method signatures
# ---------------------------------------------------------------------------


class TestMethodSignatures:
    def test_load_signature(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.load)
        params = {
            name: p.default
            for name, p in sig.parameters.items()
            if name != "self"
        }
        assert params == {
            "table_address": inspect.Parameter.empty,
            "filters": None,
            "version": None,
        }

    def test_configure_input_signature(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.configure_input)
        params = {
            name: p.default
            for name, p in sig.parameters.items()
            if name != "self"
        }
        assert params == {
            "table_address": inspect.Parameter.empty,
            "filters": None,
            "version": None,
        }

    def test_publish_signature(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.publish)
        params = {
            name: p.default
            for name, p in sig.parameters.items()
            if name != "self"
        }
        assert params == {
            "version": inspect.Parameter.empty,
            "params": None,
            "mode": None,
        }

    def test_run_signature(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.run)
        params = {
            name: p.default
            for name, p in sig.parameters.items()
            if name != "self"
        }
        assert params == {
            "version": None,
            "mode": None,
        }

    def test_schema_signature(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.schema)
        params = {
            name: p.default
            for name, p in sig.parameters.items()
            if name != "self"
        }
        assert params == {"ddl_or_path": inspect.Parameter.empty}

    def test_execute_signature(self):
        from dbport import DBPort

        sig = inspect.signature(DBPort.execute)
        params = {
            name: p.default
            for name, p in sig.parameters.items()
            if name != "self"
        }
        assert params == {"sql_or_path": inspect.Parameter.empty}


# ---------------------------------------------------------------------------
# config_only mode contract
# ---------------------------------------------------------------------------


class TestConfigOnlyContract:
    """Verify which methods are guarded and which work in config_only mode."""

    def _make_config_only(self, tmp_path: Path):
        from dbport import DBPort

        return DBPort(
            agency="test",
            dataset_id="ds",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "test.duckdb"),
            config_only=True,
        )

    @pytest.mark.parametrize("method", [
        "schema",
        "load",
        "execute",
        "configure_input",
        "run",
        "publish",
    ])
    def test_guarded_methods_raise_runtime_error(self, tmp_path: Path, method: str):
        client = self._make_config_only(tmp_path)
        with pytest.raises(RuntimeError, match="config_only"):
            # All guarded methods need at least one arg
            if method == "publish":
                getattr(client, method)(version="v1")
            elif method in ("load", "configure_input"):
                getattr(client, method)("some.table")
            elif method == "schema":
                getattr(client, method)("CREATE TABLE t (id INT)")
            elif method == "execute":
                getattr(client, method)("SELECT 1")
            elif method == "run":
                getattr(client, method)()
        client.close()

    def test_columns_meta_works_in_config_only(self, tmp_path: Path):
        client = self._make_config_only(tmp_path)
        client.columns.geo.meta(codelist_id="GEO")
        client.close()

    def test_columns_attach_works_in_config_only(self, tmp_path: Path):
        client = self._make_config_only(tmp_path)
        client.columns.geo.attach(table="test.cl_geo")
        client.close()

    def test_close_works_in_config_only(self, tmp_path: Path):
        client = self._make_config_only(tmp_path)
        client.close()  # should not raise

    def test_context_manager_works_in_config_only(self, tmp_path: Path):
        from dbport import DBPort

        with DBPort(
            agency="test",
            dataset_id="ds",
            lock_path=str(tmp_path / "dbport.lock"),
            duckdb_path=str(tmp_path / "test.duckdb"),
            config_only=True,
        ) as port:
            assert port is not None

    def test_run_hook_works_in_config_only(self, tmp_path: Path):
        client = self._make_config_only(tmp_path)
        # run_hook is a property that reads from lock — should work
        hook = client.run_hook
        assert hook is None or isinstance(hook, str)
        client.close()

    def test_no_data_dir_created(self, tmp_path: Path):
        data_dir = tmp_path / "data"
        self._make_config_only(tmp_path)
        # duckdb_path is tmp_path/test.duckdb (not in data/), check data/ not created
        assert not data_dir.exists()


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------


class TestReturnTypes:
    def _make_full_client(self, tmp_path: Path):
        import os

        from dbport import DBPort

        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        with patch.dict(os.environ, creds):
            return DBPort(
                agency="test",
                dataset_id="ds",
                lock_path=str(tmp_path / "dbport.lock"),
                duckdb_path=str(tmp_path / "test.duckdb"),
            )

    def test_load_returns_ingest_record(self, tmp_path: Path):
        from dbport.domain.entities.input import IngestRecord

        client = self._make_full_client(tmp_path)
        mock_record = IngestRecord(table_address="estat.foo")
        mock_svc = MagicMock()
        mock_svc.execute.return_value = mock_record
        with patch(
            "dbport.application.services.ingest.IngestService",
            return_value=mock_svc,
        ):
            result = client.load("estat.foo")
        assert isinstance(result, IngestRecord)
        client.close()

    def test_configure_input_returns_ingest_record(self, tmp_path: Path):
        from dbport.domain.entities.input import IngestRecord

        client = self._make_full_client(tmp_path)
        mock_record = IngestRecord(table_address="estat.foo")
        mock_svc = MagicMock()
        mock_svc.configure.return_value = mock_record
        with patch(
            "dbport.application.services.ingest.IngestService",
            return_value=mock_svc,
        ):
            result = client.configure_input("estat.foo")
        assert isinstance(result, IngestRecord)
        client.close()


# ---------------------------------------------------------------------------
# Initialization behavior contract
# ---------------------------------------------------------------------------


class TestInitBehavior:
    def test_full_mode_calls_sync_phases(self, tmp_path: Path):
        """Full-mode init calls all four sync phases."""
        import os

        from dbport import DBPort

        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        with patch.dict(os.environ, creds):
            with (
                patch.object(DBPort, "_auto_detect_schema") as mock_auto,
                patch.object(DBPort, "_sync_output_state") as mock_sync,
                patch.object(DBPort, "_update_last_fetched") as mock_fetch,
                patch.object(DBPort, "_load_inputs") as mock_load,
            ):
                client = DBPort(
                    agency="test",
                    dataset_id="ds",
                    lock_path=str(tmp_path / "dbport.lock"),
                    duckdb_path=str(tmp_path / "test.duckdb"),
                )

        mock_auto.assert_called_once()
        mock_sync.assert_called_once()
        mock_fetch.assert_called_once()
        mock_load.assert_called_once()
        client.close()

    def test_load_inputs_on_init_false_skips_input_loading(self, tmp_path: Path):
        """load_inputs_on_init=False skips the input reload phase."""
        import os

        from dbport import DBPort

        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        with patch.dict(os.environ, creds):
            with (
                patch.object(DBPort, "_auto_detect_schema"),
                patch.object(DBPort, "_sync_output_state"),
                patch.object(DBPort, "_update_last_fetched"),
                patch.object(DBPort, "_load_inputs") as mock_load,
            ):
                client = DBPort(
                    agency="test",
                    dataset_id="ds",
                    lock_path=str(tmp_path / "dbport.lock"),
                    duckdb_path=str(tmp_path / "test.duckdb"),
                    load_inputs_on_init=False,
                )

        mock_load.assert_not_called()
        client.close()

    def test_config_only_skips_all_sync(self, tmp_path: Path):
        """config_only=True skips all sync phases and adapter creation."""
        from dbport import DBPort

        with (
            patch.object(DBPort, "_auto_detect_schema") as mock_auto,
            patch.object(DBPort, "_sync_output_state") as mock_sync,
            patch.object(DBPort, "_update_last_fetched") as mock_fetch,
            patch.object(DBPort, "_load_inputs") as mock_load,
        ):
            client = DBPort(
                agency="test",
                dataset_id="ds",
                lock_path=str(tmp_path / "dbport.lock"),
                duckdb_path=str(tmp_path / "test.duckdb"),
                config_only=True,
            )

        mock_auto.assert_not_called()
        mock_sync.assert_not_called()
        mock_fetch.assert_not_called()
        mock_load.assert_not_called()
        client.close()

    def test_sync_errors_do_not_fail_init(self, tmp_path: Path):
        """Errors in sync phases are swallowed; init completes."""
        import os

        from dbport import DBPort

        creds = {
            "ICEBERG_REST_URI": "https://catalog.example.com",
            "ICEBERG_CATALOG_TOKEN": "tok",
            "ICEBERG_WAREHOUSE": "wh",
        }
        with patch.dict(os.environ, creds):
            with (
                patch.object(
                    DBPort, "_auto_detect_schema", side_effect=RuntimeError("boom")
                ),
                patch.object(DBPort, "_sync_output_state"),
                patch.object(DBPort, "_update_last_fetched"),
                patch.object(DBPort, "_load_inputs"),
            ):
                # _auto_detect_schema is called directly (not wrapped in try/except
                # at this level), but the method itself has a try/except.
                # Instead, test that the real method swallows errors.
                pass

        # Test the real private methods with injected failures
        from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter
        from dbport.adapters.secondary.lock.toml import TomlLockAdapter
        from dbport.domain.entities.dataset import Dataset

        client = DBPort.__new__(DBPort)
        client._compute = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        client._lock = TomlLockAdapter(
            tmp_path / "dbport.lock",
            model_key="test.ds",
            model_root=".",
            duckdb_path=str(tmp_path / "test.duckdb"),
        )
        client._catalog = MagicMock()
        client._dataset = Dataset(
            agency="test",
            dataset_id="ds",
            duckdb_path=str(tmp_path / "test.duckdb"),
            lock_path=str(tmp_path / "dbport.lock"),
            model_root=str(tmp_path),
        )

        # Each should swallow errors
        with patch("dbport.application.services.auto_schema.AutoSchemaService") as m:
            m.return_value.execute.side_effect = RuntimeError("fail")
            client._auto_detect_schema()  # should not raise

        with patch("dbport.application.services.sync.SyncService") as m:
            m.return_value.sync_output_table.side_effect = RuntimeError("fail")
            client._sync_output_state()  # should not raise

        with patch("dbport.application.services.sync.SyncService") as m:
            m.return_value.sync_inputs.side_effect = RuntimeError("fail")
            client._load_inputs()  # should not raise

        client._compute.close()
