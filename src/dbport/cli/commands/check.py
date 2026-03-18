"""dbp status check — verify project health."""

from __future__ import annotations

import sys

import typer

from ..errors import cli_error_handler
from ..render import print_error, print_info, print_json, print_success


def check_cmd(
    ctx: typer.Context,
    strict: bool = typer.Option(False, "--strict", help="Fail on warnings."),
) -> None:
    """Check whether the project is operationally healthy."""
    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    with cli_error_handler("status check", json_output=cli_ctx.json_output):
        checks: list[dict] = []

        # 1. Lockfile
        lock_ok = cli_ctx.lockfile_path.exists()
        checks.append(
            {
                "name": "lockfile",
                "status": "pass" if lock_ok else "fail",
                "detail": str(cli_ctx.lockfile_path),
            }
        )

        # 2. Lockfile readable
        if lock_ok:
            try:
                import tomllib

                tomllib.loads(cli_ctx.lockfile_path.read_text(encoding="utf-8"))
                checks.append(
                    {"name": "lockfile_readable", "status": "pass", "detail": "valid TOML"}
                )
            except Exception as exc:
                checks.append({"name": "lockfile_readable", "status": "fail", "detail": str(exc)})

        # 3. DuckDB
        duckdb_ok = False
        try:
            import duckdb

            conn = duckdb.connect(":memory:")
            conn.execute("SELECT 1")
            conn.close()
            duckdb_ok = True
            checks.append(
                {"name": "duckdb", "status": "pass", "detail": f"duckdb {duckdb.__version__}"}
            )
        except Exception as exc:
            checks.append({"name": "duckdb", "status": "fail", "detail": str(exc)})

        # 4. Credentials
        try:
            from ...infrastructure.credentials import WarehouseCreds

            creds = WarehouseCreds()
            missing = []
            if not creds.catalog_uri:
                missing.append("ICEBERG_REST_URI")
            if not creds.catalog_token:
                missing.append("ICEBERG_CATALOG_TOKEN")
            if not creds.warehouse:
                missing.append("ICEBERG_WAREHOUSE")
            if missing:
                checks.append(
                    {
                        "name": "credentials",
                        "status": "warn",
                        "detail": f"missing: {', '.join(missing)}",
                    }
                )
            else:
                checks.append(
                    {"name": "credentials", "status": "pass", "detail": "all required vars set"}
                )
        except Exception:
            # WarehouseCreds validation failed — required fields not resolvable
            # from any source (kwargs, .env file, or environment variables).
            checks.append(
                {
                    "name": "credentials",
                    "status": "warn",
                    "detail": (
                        "not configured"
                        " (set ICEBERG_REST_URI, ICEBERG_CATALOG_TOKEN,"
                        " ICEBERG_WAREHOUSE via .env or environment)"
                    ),
                }
            )

        # 5. Python dependencies
        dep_issues = []
        for pkg in ("pyarrow", "pyiceberg", "pydantic"):
            try:
                __import__(pkg)
            except ImportError:
                dep_issues.append(pkg)
        if dep_issues:
            checks.append(
                {
                    "name": "dependencies",
                    "status": "fail",
                    "detail": f"missing: {', '.join(dep_issues)}",
                }
            )
        else:
            checks.append({"name": "dependencies", "status": "pass", "detail": "all present"})

        # Summarize
        has_fail = any(c["status"] == "fail" for c in checks)
        has_warn = any(c["status"] == "warn" for c in checks)
        overall_ok = not has_fail and (not has_warn if strict else True)

        if cli_ctx.json_output:
            print_json("status check", {"checks": checks, "ok": overall_ok}, ok=overall_ok)
            if not overall_ok:
                sys.exit(1)
            return

        for c in checks:
            status = c["status"]
            name = c["name"]
            detail = c["detail"]
            if status == "pass":
                print_info(f"  [green]PASS[/]  {name}: {detail}")
            elif status == "warn":
                print_info(f"  [yellow]WARN[/]  {name}: {detail}")
            else:
                print_info(f"  [red]FAIL[/]  {name}: {detail}")

        print_info("")
        if overall_ok:
            print_success("All checks passed.")
        else:
            print_error("Some checks failed." + (" (strict mode)" if strict else ""))
            sys.exit(1)
