"""dbp config — repo-level control plane.

``dbp config KEY [VALUE]`` — get or set a configuration value.

Supported keys: ``default``, ``folder``, ``run-hook``, ``check``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import typer

from ..errors import cli_error_handler
from ..render import print_error, print_info, print_json, print_success, print_warning

config_app = typer.Typer(
    name="config",
    help="Get/set project configuration or check project health.",
    no_args_is_help=True,
)

# Valid config keys
_CONFIG_KEYS = ("default", "folder", "run-hook", "check")


@config_app.callback(invoke_without_command=True)
def config_callback(
    ctx: typer.Context,
    key: str = typer.Argument(..., help=f"Config key: {', '.join(_CONFIG_KEYS)}."),
    value: str | None = typer.Argument(None, help="Value to set (omit to show current value)."),
) -> None:
    """Get or set project configuration values.

    Supported keys: default, folder, run-hook, check.
    """
    if key not in _CONFIG_KEYS:
        print_error(f"Unknown config key: '{key}'. Valid keys: {', '.join(_CONFIG_KEYS)}")
        raise typer.Exit(1)

    from ..main import get_cli_ctx

    cli_ctx = get_cli_ctx(ctx)

    if key == "check":
        strict = value == "--strict" if value else False
        _handle_check(cli_ctx, strict)
    else:
        with cli_error_handler(f"config {key}", json_output=cli_ctx.json_output):
            if key == "default":
                _handle_default(cli_ctx, value)
            elif key == "folder":
                _handle_folder(cli_ctx, value)
            elif key == "run-hook":
                _handle_run_hook(cli_ctx, value)


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------

def _handle_check(cli_ctx, strict: bool) -> None:
    with cli_error_handler("check", json_output=cli_ctx.json_output):
        checks: list[dict] = []

        # 1. Lockfile
        lock_ok = cli_ctx.lockfile_path.exists()
        checks.append({
            "name": "lockfile",
            "status": "pass" if lock_ok else "fail",
            "detail": str(cli_ctx.lockfile_path),
        })

        # 2. Lockfile readable
        if lock_ok:
            try:
                import tomllib
                tomllib.loads(cli_ctx.lockfile_path.read_text(encoding="utf-8"))
                checks.append({"name": "lockfile_readable", "status": "pass", "detail": "valid TOML"})
            except Exception as exc:
                checks.append({"name": "lockfile_readable", "status": "fail", "detail": str(exc)})

        # 3. DuckDB
        try:
            import duckdb
            conn = duckdb.connect(":memory:")
            conn.execute("SELECT 1")
            conn.close()
            checks.append({"name": "duckdb", "status": "pass", "detail": f"duckdb {duckdb.__version__}"})
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
                checks.append({
                    "name": "credentials",
                    "status": "warn",
                    "detail": f"missing: {', '.join(missing)}",
                })
            else:
                checks.append({"name": "credentials", "status": "pass", "detail": "all required vars set"})
        except Exception:
            import os
            missing = []
            for var in ("ICEBERG_REST_URI", "ICEBERG_CATALOG_TOKEN", "ICEBERG_WAREHOUSE"):
                if not os.environ.get(var):
                    missing.append(var)
            checks.append({
                "name": "credentials",
                "status": "warn",
                "detail": f"missing env vars: {', '.join(missing)}" if missing else "validation failed",
            })

        # 5. Python dependencies
        dep_issues = []
        for pkg in ("pyarrow", "pyiceberg", "pydantic"):
            try:
                __import__(pkg)
            except ImportError:
                dep_issues.append(pkg)
        if dep_issues:
            checks.append({"name": "dependencies", "status": "fail", "detail": f"missing: {', '.join(dep_issues)}"})
        else:
            checks.append({"name": "dependencies", "status": "pass", "detail": "all present"})

        # Summarize
        has_fail = any(c["status"] == "fail" for c in checks)
        has_warn = any(c["status"] == "warn" for c in checks)
        overall_ok = not has_fail and (not has_warn if strict else True)

        if cli_ctx.json_output:
            print_json("check", {"checks": checks, "ok": overall_ok}, ok=overall_ok)
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


# ---------------------------------------------------------------------------
# Key handlers
# ---------------------------------------------------------------------------

def _handle_default(cli_ctx, value: str | None) -> None:
    from ..context import read_default_model, read_lock_models, write_default_model

    if value is None:
        current = read_default_model(cli_ctx.lockfile_path)
        if cli_ctx.json_output:
            print_json("config default", {"default_model": current})
        elif current:
            print_info(f"Default model: {current}")
        else:
            print_info("No default model set.")
    else:
        models = read_lock_models(cli_ctx.lockfile_path)
        if value not in models:
            available = list(models.keys()) if models else []
            print_error(
                f"Model '{value}' not found in {cli_ctx.lockfile_path}. "
                f"Available: {available}"
            )
            raise typer.Exit(1)
        write_default_model(cli_ctx.lockfile_path, value)
        if cli_ctx.json_output:
            print_json("config default", {"default_model": value})
        else:
            print_success(f"Default model set to: {value}")


def _handle_folder(cli_ctx, value: str | None) -> None:
    from ..context import read_models_folder, write_models_folder

    if value is None:
        current = read_models_folder(cli_ctx.lockfile_path)
        if cli_ctx.json_output:
            print_json("config folder", {"models_folder": current})
        else:
            print_info(f"Models folder: {current}")
    else:
        folder = value.strip("/")
        write_models_folder(cli_ctx.lockfile_path, folder)
        if cli_ctx.json_output:
            print_json("config folder", {"models_folder": folder})
        else:
            print_success(f"Models folder set to: {folder}")


def _handle_run_hook(cli_ctx, value: str | None) -> None:
    from ..context import resolve_model_paths

    if value is None:
        paths = resolve_model_paths(cli_ctx)
        from ...adapters.secondary.lock.toml import TomlLockAdapter

        model_key = f"{paths.agency}.{paths.dataset_id}"
        raw_root = str(Path(paths.model_root).relative_to(cli_ctx.project_path)) if Path(paths.model_root).is_absolute() else paths.model_root
        raw_db = str(Path(paths.duckdb_path).relative_to(cli_ctx.project_path)) if Path(paths.duckdb_path).is_absolute() else paths.duckdb_path
        adapter = TomlLockAdapter(
            cli_ctx.lockfile_path,
            model_key=model_key,
            model_root=raw_root,
            duckdb_path=raw_db,
        )
        current = adapter.read_run_hook()
        if cli_ctx.json_output:
            print_json("config run-hook", {"run_hook": current, "model": model_key})
        elif current:
            print_info(f"Run hook for {model_key}: {current}")
        else:
            print_info(f"No run hook set for {model_key}.")
    else:
        paths = resolve_model_paths(cli_ctx)
        from ...adapters.secondary.lock.toml import TomlLockAdapter

        model_key = f"{paths.agency}.{paths.dataset_id}"
        raw_root = str(Path(paths.model_root).relative_to(cli_ctx.project_path)) if Path(paths.model_root).is_absolute() else paths.model_root
        raw_db = str(Path(paths.duckdb_path).relative_to(cli_ctx.project_path)) if Path(paths.duckdb_path).is_absolute() else paths.duckdb_path
        adapter = TomlLockAdapter(
            cli_ctx.lockfile_path,
            model_key=model_key,
            model_root=raw_root,
            duckdb_path=raw_db,
        )
        # Normalize hook_path: resolve relative to CWD, store relative to model_root
        abs_model_root = (cli_ctx.project_path / raw_root).resolve()
        hook = Path(value)
        if not hook.is_absolute():
            hook = (Path.cwd() / hook).resolve()
        try:
            normalized = str(hook.relative_to(abs_model_root))
        except ValueError:
            normalized = value
        adapter.write_run_hook(normalized)

        if cli_ctx.json_output:
            print_json("config run-hook", {"run_hook": normalized, "model": model_key})
        else:
            print_success(f"Run hook set to: {normalized}")
