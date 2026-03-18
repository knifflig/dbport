# Hooks & Execution

DBPort supports hook-based execution for model logic. A hook is a Python or SQL file that serves as the model's main entry point.

## Running a model

=== "CLI"

    ```bash
    # Execute the hook only (no publish)
    dbp model exec

    # Full lifecycle: execute hook + publish
    dbp model run --version 2026-03-09 --timing

    # Execute a specific file instead of the configured hook
    dbp model exec --target sql/transform.sql
    ```

=== "Python"

    ```python
    # Execute the hook only
    port.run()

    # Execute hook + publish
    port.run(version="2026-03-09")
    ```

## Hook resolution order

!!! note "Resolution precedence"

    When no explicit target is given, DBPort resolves the hook in this order:

    1. **Configured hook path** — set via `dbp config default hook`
    2. **`main.py`** in the model root — auto-detected if it exists
    3. **`sql/main.sql`** in the model root — legacy fallback
    4. **Default `main.py`** — used when no model root is available; errors at execution if the file does not exist

## Python hooks

Python hooks are executed with `port` (the active `DBPort` instance) available in the namespace.

```python
# main.py
from dbport import DBPort


def run(port: DBPort) -> None:  # (1)!
    port.schema("sql/create_output.sql")
    port.load("estat.nama_10r_3empers", filters={"wstatus": "EMP"})
    port.execute("sql/transform.sql")


if __name__ == "__main__":  # (2)!
    with DBPort(agency="test", dataset_id="output") as port:
        run(port)
        port.publish(version="2026-03-15")
```

1. If a top-level `run(port)` function is defined, it is called automatically after the module is loaded. This is the recommended pattern.
2. The `if __name__ == "__main__"` block runs only when the file is executed directly. During CLI execution, `__name__` is set to `"__dbport_hook__"`, so this block is skipped.

### Inline execution

If no `run(port)` function is defined, the module body is executed directly with `port` in scope:

```python
# main.py — inline style (simpler, but less reusable)
port.schema("sql/create_output.sql")
port.load("estat.data_table")
port.execute("sql/transform.sql")
```

## SQL hooks

SQL hooks are passed directly to `port.execute()`. Use this for models that are pure SQL with no Python logic:

```sql
-- sql/main.sql
CREATE OR REPLACE TABLE output.result AS
SELECT geo, year, SUM(value) AS total
FROM input.raw_data
GROUP BY geo, year;
```

## `exec` vs `run` vs `publish`

These three operations are distinct and composable:

=== "CLI"

    | Command | What it does | Publishes data? |
    |---|---|---|
    | `dbp model exec` | Execute the configured hook | No |
    | `dbp model run --version V` | Execute hook, then publish | Yes |
    | `dbp model publish --version V` | Publish only (no execution) | Yes |

    `dbp model run` is the full lifecycle command: sync → execute → publish.

=== "Python"

    | Method | What it does | Publishes data? |
    |---|---|---|
    | `port.execute(sql_or_path)` | Run a single SQL statement or file | No |
    | `port.run(version="V")` | Resolve and execute the hook, then publish | Only if `version` given |
    | `port.publish(version="V")` | Write the output table to the warehouse | Yes |

## Trust model

!!! warning "Hooks are trusted code"

    Hook files run with the same permissions as the calling script or CLI process. There is no sandboxing — the hook is your model logic, authored by the model owner. This is the same trust boundary as any other user-authored Python module.
