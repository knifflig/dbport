"""Tests for adapters.secondary.compute.duckdb."""

from __future__ import annotations

from pathlib import Path

import pytest

from dbport.adapters.secondary.compute.duckdb import DuckDBComputeAdapter, _INIT_SCHEMAS


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
