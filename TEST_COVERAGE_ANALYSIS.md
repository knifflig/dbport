# Test Coverage Analysis

**Date:** 2026-03-19
**Tests:** 1,021 | **Line coverage:** 100% | **Branch coverage:** 99.17% (34 partial branches uncovered)

## Current State

The test suite is strong: 100% line coverage, 99.17% branch coverage, fast execution (~38s), no mocks (uses `_Fake*` doubles), and a clean mirror of the source layout. The gaps below are qualitative — areas where the tests could be deeper or more realistic despite already hitting every line.

---

## 1. Missing CLI Command Test Files

**Priority: High**

Two CLI command modules have **no dedicated test files**:

| Source file | Lines | Test file |
|---|---|---|
| `src/dbport/cli/commands/lifecycle.py` | 273 | **None** |
| `src/dbport/cli/commands/model.py` | 196 | **None** |

These modules are exercised indirectly through other CLI tests (e.g., `test_run.py`, `test_cli_contract.py`), which is why line coverage shows 100%. However, there are no focused tests for:

- `lifecycle._run_execute_step()` callback behavior (success/failure progress reporting)
- `lifecycle._load_step()` / `_publish_step()` orchestration
- `model_sync_cmd`, `model_run_cmd`, `model_load_cmd` — argument parsing, error formatting, JSON output mode
- Edge cases: missing model key resolution, invalid model arguments, `--json` flag formatting

**Recommendation:** Add `tests/test_dbport/cli/test_lifecycle.py` and `tests/test_dbport/cli/test_model.py` with focused unit tests for each command and helper function.

---

## 2. Uncovered Branch Paths in `iceberg.py`

**Priority: High**

`iceberg.py` has 17 partial branches uncovered (the most of any file). Key gaps:

| Lines | Description |
|---|---|
| `46->51` | `_write_table_properties` fallback when transaction API is missing but `update_properties` exists |
| `108->110`, `110->112`, `112->114`, `114->117` | Optional S3 credential fields being `None` — tests always provide all creds |
| `156->160`, `160->170`, `164->166` | `_ensure_warehouse_attached` edge cases |
| `252->251` | `resolve_input_snapshot` loop falling through without finding a matching version |
| `311->318` | `_snapshot_timestamp_from_table` — `current_snapshot()` returns a snapshot whose ID doesn't match |
| `344->346` | `_estimate_ingest_total_rows` — `snapshot_id is None` path |
| `546->548`, `568->562` | `_iter_batches_with_progress` with `None` callback; `_snapshot_summary` iterable edge case |
| `595->600`, `603->608` | `_estimate_ingest_total_rows` fallback when `snapshot_by_id` exists but returns `None` |
| `856->851` | `_write_via_streaming_arrow` — batch skip loop exhausting iterator before reaching committed count |

**Recommendation:** Add targeted tests for each branch, especially:
- Credentials with partial S3 config (only `s3_endpoint` set, others `None`)
- `_write_table_properties` fallback path via `update_properties()`
- Streaming arrow batch-skip exhaustion during checkpoint recovery

---

## 3. Publish Service — Limited Scenario Coverage

**Priority: Medium**

`test_publish.py` (application services) has 18 tests, but several real-world scenarios are untested:

- **Metadata attachment failures** — What happens if `attach_to_table` raises after data is already written? The test suite doesn't verify that the version record is still persisted or that partial state is handled.
- **Codelist generation errors** — No tests for `generate_codelist_bytes` raising when a column referenced by `.attach()` doesn't exist in DuckDB.
- **Multiple sequential publishes** — Tests verify single publish and idempotent skip, but not publish → refresh → publish (version lifecycle).
- **Params serialization edge cases** — Empty params `{}` vs `None` vs params with special characters.

**Recommendation:** Add scenario tests that exercise the full publish pipeline with error injection at each stage.

---

## 4. No Integration/Smoke Tests for the `DBPort` Context Manager Flow

**Priority: Medium**

The `DBPort` class has unit tests for individual methods, but no test exercises the full lifecycle:

```python
with DBPort(...) as port:
    port.schema(...)
    port.load(...)
    port.execute(...)
    port.publish(...)
```

All tests call methods in isolation with pre-configured fakes. This means:
- **State ordering bugs** are not caught (e.g., calling `execute()` before `schema()`)
- **Resource cleanup** on exceptions mid-pipeline is untested
- **Lock file consistency** after a full schema→load→execute→publish cycle is unverified

**Recommendation:** Add a `test_client_integration.py` with end-to-end tests using in-memory fakes that exercise the full pipeline in order, including error scenarios mid-flow.

---

## 5. Transform Service — No Error Path Tests

**Priority: Medium**

`test_transform.py` has 11 tests covering happy paths (inline SQL, file routing, path traversal rejection). Missing:

- **SQL syntax errors** — Does `execute()` propagate DuckDB errors cleanly, or does it swallow/wrap them?
- **File not found** — What happens when a `.sql` path doesn't exist?
- **Empty SQL file** — Edge case for file with no content or only whitespace.
- **Encoding issues** — SQL files with non-UTF-8 encoding.

**Recommendation:** Add error-path tests to verify exception types and messages.

---

## 6. `AutoSchemaService` — Arrow-to-DuckDB Type Mapping

**Priority: Medium**

`test_auto_schema.py` tests 11 Arrow type conversions, but the `_arrow_type_to_duckdb` function handles many more types. Untested mappings:

- Nested types: `list<struct<...>>`, `map<string, int>`
- Decimal types with various precision/scale
- Duration/interval types
- Large string/binary variants
- Null type

**Recommendation:** Add parametrized tests for all Arrow type variants, especially nested and uncommon types.

---

## 7. Sync Service — Thin Coverage

**Priority: Medium**

`test_sync.py` has only 5 tests for a 106-line service. Gaps:

- **Sync when warehouse table doesn't exist** — Does it skip gracefully or error?
- **Sync with schema drift** — Local schema diverged from warehouse; is drift detected?
- **Sync with stale inputs** — Input snapshot changed since last load; does it re-sync?
- **Multiple inputs sync** — Only one input tested at a time.

**Recommendation:** Expand `test_sync.py` to cover each edge case, especially the interplay between `sync_output_table` and `sync_inputs`.

---

## 8. `WarehouseCreds` — Minimal Validation Testing

**Priority: Low**

`test_credentials.py` has 8 tests but doesn't cover:

- Credentials with only required fields (no S3 config at all)
- Invalid URI formats (non-URL strings for `catalog_uri`)
- Environment variable precedence when both kwargs and env vars are set
- `.env` file loading from the `.claude/` directory

**Recommendation:** Add parametrized tests for credential resolution order and validation edge cases.

---

## 9. Lock File — Concurrent Write Safety

**Priority: Low**

`TomlLockAdapter` does atomic writes (write-to-temp + rename), and `test_toml.py` has 3 tests for atomic write error handling. However:

- **No concurrent write test** — Two writers updating the lock simultaneously
- **No corruption recovery** — What happens if the temp file exists from a previous crash?
- **Large lock files** — Performance with hundreds of models/versions

**Recommendation:** Add a stress test or at least a test that simulates a pre-existing temp file scenario.

---

## 10. Progress/Callback Infrastructure — Untested Context Variable Propagation

**Priority: Low**

`infrastructure/progress.py` uses `contextvars.ContextVar` for propagating progress callbacks. Tests verify the callback protocol but don't test:

- Context variable isolation across threads
- Nested `progress_phase` contexts
- Callback propagation into sub-services (e.g., progress from `publish` → `write_versioned` → `_write_via_streaming_arrow`)

**Recommendation:** Add tests verifying callback propagation through the service call chain.

---

## Summary: Recommended Action Items

| # | Area | Priority | Estimated tests to add |
|---|---|---|---|
| 1 | CLI `lifecycle.py` + `model.py` test files | High | ~20–25 |
| 2 | `iceberg.py` branch coverage | High | ~12–15 |
| 3 | Publish service scenarios | Medium | ~8–10 |
| 4 | `DBPort` integration tests | Medium | ~5–8 |
| 5 | Transform error paths | Medium | ~5–6 |
| 6 | `AutoSchemaService` type mappings | Medium | ~8–10 |
| 7 | Sync service edge cases | Medium | ~5–8 |
| 8 | Credential validation | Low | ~4–5 |
| 9 | Lock file concurrency | Low | ~3–4 |
| 10 | Progress context propagation | Low | ~3–4 |

Total: ~75–95 new tests to bring the suite from "excellent line coverage" to "comprehensive behavioral coverage."
