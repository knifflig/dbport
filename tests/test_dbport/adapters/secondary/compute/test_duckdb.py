"""Tests for adapters.secondary.compute.duckdb."""

from __future__ import annotations

from pathlib import Path

import pytest

from dbport.adapters.secondary.compute.duckdb import _INIT_SCHEMAS, DuckDBComputeAdapter


@pytest.fixture
def adapter(tmp_path: Path) -> DuckDBComputeAdapter:
    db_path = tmp_path / "test.duckdb"
    ad = DuckDBComputeAdapter(db_path)
    yield ad
    ad.close()


class TestDuckDBComputeAdapterInit:
    def test_stores_path(self, tmp_path: Path):
        path = tmp_path / "dbport.duckdb"
        ad = DuckDBComputeAdapter(path)
        assert ad._path == path
        ad.close()

    def test_connection_lazy_before_first_use(self, tmp_path: Path):
        ad = DuckDBComputeAdapter(tmp_path / "dbport.duckdb")
        assert ad._con is None
        ad.close()

    def test_connection_established_on_first_execute(self, adapter: DuckDBComputeAdapter):
        adapter.execute("SELECT 1")
        assert adapter._con is not None

    def test_standard_schemas_created(self, adapter: DuckDBComputeAdapter):
        adapter.execute("SELECT 1")  # trigger connection
        for schema in _INIT_SCHEMAS:
            assert adapter.relation_exists(schema, "__nonexistent__") is False
            # Schema exists if relation_exists doesn't raise on it
            rows = adapter.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?",
                [schema],
            ).fetchall()
            assert rows, f"Schema '{schema}' was not created"

    def test_file_backed_db_persists(self, tmp_path: Path):
        path = tmp_path / "persist.duckdb"
        ad = DuckDBComputeAdapter(path)
        ad.execute("CREATE TABLE inputs.t (x INT)")
        ad.execute("INSERT INTO inputs.t VALUES (42)")
        ad.close()

        ad2 = DuckDBComputeAdapter(path)
        result = ad2.execute("SELECT x FROM inputs.t").fetchone()
        assert result == (42,)
        ad2.close()


class TestDuckDBComputeAdapterExecute:
    def test_execute_returns_result(self, adapter: DuckDBComputeAdapter):
        result = adapter.execute("SELECT 1 + 1").fetchone()
        assert result == (2,)

    def test_execute_with_parameters(self, adapter: DuckDBComputeAdapter):
        result = adapter.execute("SELECT ? + ?", [3, 4]).fetchone()
        assert result == (7,)

    def test_execute_ddl(self, adapter: DuckDBComputeAdapter):
        adapter.execute("CREATE TABLE inputs.foo (id INT, val VARCHAR)")
        assert adapter.relation_exists("inputs", "foo")

    def test_execute_insert_and_select(self, adapter: DuckDBComputeAdapter):
        adapter.execute("CREATE TABLE inputs.nums (n INT)")
        adapter.execute("INSERT INTO inputs.nums VALUES (10), (20), (30)")
        rows = adapter.execute("SELECT SUM(n) FROM inputs.nums").fetchone()
        assert rows == (60,)

    def test_execute_file(self, adapter: DuckDBComputeAdapter, tmp_path: Path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            "CREATE TABLE inputs.from_file (x INT);\nINSERT INTO inputs.from_file VALUES (99);",
            encoding="utf-8",
        )
        adapter.execute_file(str(sql_file))
        result = adapter.execute("SELECT x FROM inputs.from_file").fetchone()
        assert result == (99,)


class TestDuckDBComputeAdapterRelationExists:
    def test_returns_false_when_not_exists(self, adapter: DuckDBComputeAdapter):
        assert adapter.relation_exists("inputs", "nonexistent") is False

    def test_returns_true_after_create(self, adapter: DuckDBComputeAdapter):
        adapter.execute("CREATE TABLE inputs.my_table (id INT)")
        assert adapter.relation_exists("inputs", "my_table") is True

    def test_case_sensitive_table_name(self, adapter: DuckDBComputeAdapter):
        adapter.execute("CREATE TABLE inputs.lower_table (id INT)")
        # DuckDB uses lowercase internally
        assert adapter.relation_exists("inputs", "lower_table") is True
        assert adapter.relation_exists("inputs", "LOWER_TABLE") is False

    def test_wrong_schema(self, adapter: DuckDBComputeAdapter):
        adapter.execute("CREATE TABLE inputs.my_table (id INT)")
        assert adapter.relation_exists("staging", "my_table") is False


class TestDuckDBComputeAdapterArrowBatches:
    def test_to_arrow_batches_returns_reader(self, adapter: DuckDBComputeAdapter):
        adapter.execute("CREATE TABLE inputs.data (n INT)")
        adapter.execute("INSERT INTO inputs.data VALUES (1), (2), (3)")
        reader = adapter.to_arrow_batches("SELECT n FROM inputs.data ORDER BY n")
        batch = reader.read_next_batch()
        assert batch.num_rows == 3
        assert batch.column(0).to_pylist() == [1, 2, 3]

    def test_to_arrow_batches_batch_size(self, adapter: DuckDBComputeAdapter):
        adapter.execute("CREATE TABLE inputs.big (n INT)")
        adapter.execute(
            "INSERT INTO inputs.big SELECT range FROM range(1, 101)"
        )
        reader = adapter.to_arrow_batches("SELECT n FROM inputs.big ORDER BY n", batch_size=25)
        total = 0
        for batch in reader:
            total += batch.num_rows
        assert total == 100


class TestDuckDBComputeAdapterArrowRegistration:
    def test_register_and_unregister_arrow(self, adapter: DuckDBComputeAdapter):
        """register_arrow and unregister_arrow work with Arrow tables."""
        import pyarrow as pa

        arrow_table = pa.table({"x": [1, 2, 3]})
        adapter.register_arrow("test_view", arrow_table)
        result = adapter.execute("SELECT COUNT(*) FROM test_view").fetchone()
        assert result == (3,)
        adapter.unregister_arrow("test_view")

    def test_duckdb_import_error_raises_runtime_error(self, monkeypatch, tmp_path):
        """RuntimeError raised when duckdb is not installed."""
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "duckdb":
                raise ImportError("No module named 'duckdb'")
            return real_import(name, *args, **kwargs)

        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        monkeypatch.setattr(builtins, "__import__", mock_import)
        with pytest.raises(RuntimeError, match="duckdb is required"):
            ad._get_con()


class TestDuckDBComputeAdapterCloseEdgeCases:
    def test_close_suppresses_exception(self, tmp_path: Path):
        """close() suppresses exceptions from the underlying connection."""
        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        ad.execute("SELECT 1")
        # Sabotage the connection to trigger an exception on close
        ad._con = type("BrokenCon", (), {"close": lambda self: (_ for _ in ()).throw(RuntimeError("broken"))})()
        ad.close()  # should not raise
        assert ad._con is None


class TestEnsureExtensionsFallback:
    def test_load_failure_triggers_install_then_load(self, tmp_path: Path, monkeypatch):
        """When LOAD fails, ensure_extensions tries DuckDB INSTALL via HTTPS."""
        from unittest.mock import MagicMock

        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        mock_con = MagicMock()
        call_log: list[str] = []

        def tracking_execute(sql, *args, **kwargs):
            call_log.append(sql)
            # First LOAD of each ext fails, INSTALL succeeds, second LOAD succeeds
            if sql.startswith("LOAD") and sum(1 for c in call_log if c == sql) == 1:
                raise Exception("extension not found")

        mock_con.execute = tracking_execute
        ad._con = mock_con
        monkeypatch.setattr(ad, "_get_con", lambda: mock_con)

        ad.ensure_extensions()
        assert any("INSTALL" in c for c in call_log)
        # Verify HTTPS repository is set before INSTALL
        assert any("custom_extension_repository" in c and "https://" in c for c in call_log)
        ad._con = None

    def test_python_download_fallback(self, tmp_path: Path, monkeypatch):
        """When DuckDB INSTALL also fails, falls back to Python urllib download."""
        from unittest.mock import MagicMock, patch

        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        mock_con = MagicMock()
        call_log: list[str] = []
        download_calls: list[str] = []

        def tracking_execute(sql, *args, **kwargs):
            call_log.append(sql)
            # First LOAD fails, INSTALL fails, second LOAD (after download) succeeds
            if sql.startswith("LOAD"):
                load_count = sum(1 for c in call_log if c == sql)
                if load_count == 1:
                    raise Exception("extension not found")
            if sql.startswith("INSTALL"):
                raise Exception("download blocked")

        mock_con.execute = tracking_execute
        ad._con = mock_con
        monkeypatch.setattr(ad, "_get_con", lambda: mock_con)

        def mock_download(ext):
            download_calls.append(ext)

        monkeypatch.setattr(DuckDBComputeAdapter, "_download_extension", staticmethod(mock_download))

        ad.ensure_extensions()
        # All 3 extensions should trigger the download fallback
        assert len(download_calls) == 3
        ad._con = None

    def test_all_strategies_fail_raises_runtime_error(self, tmp_path: Path, monkeypatch):
        """When LOAD, INSTALL, and download all fail, RuntimeError is raised."""
        from unittest.mock import MagicMock

        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        mock_con = MagicMock()

        def always_fail(sql, *args, **kwargs):
            raise Exception("not available")

        mock_con.execute = always_fail
        ad._con = mock_con
        monkeypatch.setattr(ad, "_get_con", lambda: mock_con)
        monkeypatch.setattr(
            DuckDBComputeAdapter, "_download_extension",
            staticmethod(lambda ext: (_ for _ in ()).throw(Exception("download failed"))),
        )

        with pytest.raises(RuntimeError, match="could not be loaded"):
            ad.ensure_extensions()
        ad._con = None


class TestDuckDBComputeAdapterClose:
    def test_close_sets_con_to_none(self, tmp_path: Path):
        ad = DuckDBComputeAdapter(tmp_path / "dbport.duckdb")
        ad.execute("SELECT 1")
        assert ad._con is not None
        ad.close()
        assert ad._con is None

    def test_close_is_idempotent(self, tmp_path: Path):
        ad = DuckDBComputeAdapter(tmp_path / "dbport.duckdb")
        ad.close()
        ad.close()  # second close must not raise


class TestDownloadExtension:
    def test_download_extension_creates_file(self, tmp_path, monkeypatch):
        import gzip
        import urllib.request
        from unittest.mock import MagicMock

        import duckdb as real_duckdb

        monkeypatch.setattr(real_duckdb, "__version__", "1.0.0")
        fake_data = b"fake_extension_binary"
        compressed = gzip.compress(fake_data)
        mock_resp = MagicMock()
        mock_resp.read.return_value = compressed
        monkeypatch.setattr(urllib.request, "urlopen", lambda url, timeout=60: mock_resp)
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

        DuckDBComputeAdapter._download_extension("testext")

        ext_file = (
            tmp_path / ".duckdb" / "extensions" / "v1.0.0" / "linux_amd64" / "testext.duckdb_extension"
        )
        assert ext_file.exists()
        assert ext_file.read_bytes() == fake_data

    def test_download_extension_skips_if_exists(self, tmp_path, monkeypatch):
        import duckdb as real_duckdb

        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.setattr(real_duckdb, "__version__", "1.0.0")
        ext_dir = tmp_path / ".duckdb" / "extensions" / "v1.0.0" / "linux_amd64"
        ext_dir.mkdir(parents=True)
        dest = ext_dir / "testext.duckdb_extension"
        dest.write_bytes(b"existing")

        DuckDBComputeAdapter._download_extension("testext")

        assert dest.read_bytes() == b"existing"  # unchanged


class TestEnsureExtensionsLoadHappyPath:
    def test_load_succeeds_without_install(self, tmp_path: Path, monkeypatch):
        """When LOAD succeeds on the first try, the continue branch (line 60) is hit."""
        from unittest.mock import MagicMock

        ad = DuckDBComputeAdapter(tmp_path / "test.duckdb")
        mock_con = MagicMock()
        call_log: list[str] = []

        def tracking_execute(sql, *args, **kwargs):
            call_log.append(sql)
            # All LOADs succeed immediately
            return None

        mock_con.execute = tracking_execute
        ad._con = mock_con
        monkeypatch.setattr(ad, "_get_con", lambda: mock_con)

        ad.ensure_extensions()

        # Only LOAD calls should appear (no INSTALL, no SET, no download)
        assert all(c.startswith("LOAD") for c in call_log)
        assert len(call_log) == 3  # one LOAD per required extension
        ad._con = None
