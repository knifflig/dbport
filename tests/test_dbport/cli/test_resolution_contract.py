"""Contract tests for model selection and version resolution precedence.

These tests lock the deterministic resolution order for ``resolve_model_key``
and the two ``resolve_publish_version*`` functions, so the CLI UX contract is
stable for 0.1.0.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dbport.cli.commands.lifecycle import (
    resolve_publish_mode,
    resolve_publish_version,
    resolve_publish_version_for_publish,
)
from dbport.cli.context import (
    CliContext,
    resolve_model_key,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_multi_model_lock(tmp_path: Path, *, default_model: str | None = None) -> Path:
    """Write a lock file with two models."""
    lock = tmp_path / "dbport.lock"
    lines = []
    if default_model:
        lines.append(f'default_model = "{default_model}"\n')
    lines.append(
        '[models."a.first"]\nagency = "a"\ndataset_id = "first"\nmodel_root = "models/first"\n'
    )
    lines.append(
        '[models."b.second"]\nagency = "b"\ndataset_id = "second"\nmodel_root = "models/second"\n'
    )
    lock.write_text("\n".join(lines))
    return lock


def _write_versioned_lock(
    tmp_path: Path,
    *,
    configured_version: str | None = None,
    completed_versions: list[str] | None = None,
) -> Path:
    """Write a lock file with one model and optional version data."""
    lock = tmp_path / "dbport.lock"
    lines = [
        '[models."a.x"]\n',
        'agency = "a"\n',
        'dataset_id = "x"\n',
        'model_root = "."\n',
    ]
    if configured_version:
        lines.append(f'version = "{configured_version}"\n')
    for v in completed_versions or []:
        lines.append('\n[[models."a.x".versions]]\n')
        lines.append(f'version = "{v}"\n')
        lines.append("completed = true\n")
    lock.write_text("".join(lines))
    return lock


# ---------------------------------------------------------------------------
# Model resolution precedence
# ---------------------------------------------------------------------------


class TestModelResolutionPrecedence:
    """Lock the 5-step model resolution order."""

    def test_step1_positional_arg_wins_over_all(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test positional arg wins over all."""
        lock = _write_multi_model_lock(tmp_path, default_model="a.first")
        (tmp_path / "models" / "first").mkdir(parents=True)
        monkeypatch.chdir(tmp_path / "models" / "first")  # CWD matches a.first
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        key, _ = resolve_model_key(ctx, "b.second")
        assert key == "b.second"

    def test_step2_model_flag_wins_over_cwd_default_first(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test model flag wins over cwd, default, and first."""
        lock = _write_multi_model_lock(tmp_path, default_model="a.first")
        (tmp_path / "models" / "first").mkdir(parents=True)
        monkeypatch.chdir(tmp_path / "models" / "first")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock, model_dir="models/second")
        key, _ = resolve_model_key(ctx)
        assert key == "b.second"

    def test_step3_cwd_wins_over_default_and_first(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test CWD wins over default and first."""
        lock = _write_multi_model_lock(tmp_path, default_model="a.first")
        (tmp_path / "models" / "second").mkdir(parents=True)
        monkeypatch.chdir(tmp_path / "models" / "second")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        key, _ = resolve_model_key(ctx)
        assert key == "b.second"

    def test_step4_default_model_wins_over_first(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test default model wins over first."""
        lock = _write_multi_model_lock(tmp_path, default_model="b.second")
        monkeypatch.chdir(tmp_path)  # CWD is repo root, no model_root match
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        key, _ = resolve_model_key(ctx)
        assert key == "b.second"

    def test_step5_first_model_as_last_resort(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test first model as last resort."""
        lock = _write_multi_model_lock(tmp_path)  # no default_model
        monkeypatch.chdir(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        key, _ = resolve_model_key(ctx)
        assert key == "a.first"

    def test_unknown_positional_falls_through(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Positional arg that doesn't match any model falls through to step 2+."""
        lock = _write_multi_model_lock(tmp_path, default_model="b.second")
        monkeypatch.chdir(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        key, _ = resolve_model_key(ctx, "nonexistent.model")
        assert key == "b.second"  # falls to step 4

    def test_model_flag_not_found_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test Model flag not found raises."""
        lock = _write_multi_model_lock(tmp_path)
        monkeypatch.chdir(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock, model_dir="nonexistent")
        with pytest.raises(RuntimeError, match="No model with model_root"):
            resolve_model_key(ctx)

    def test_no_models_raises(self, tmp_path: Path) -> None:
        """Test No models raises."""
        lock = tmp_path / "dbport.lock"
        lock.write_text("# empty\n")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        with pytest.raises(RuntimeError, match="No models found"):
            resolve_model_key(ctx)


# ---------------------------------------------------------------------------
# Version resolution for ``dbp model run``
# ---------------------------------------------------------------------------


class TestResolvePublishVersion:
    """Lock the 3-step version resolution for ``dbp model run``."""

    def test_step1_explicit_version_wins(self, tmp_path: Path) -> None:
        """Test Step1 explicit version wins."""
        lock = _write_versioned_lock(
            tmp_path, configured_version="2026-01-01", completed_versions=["2025-12-01"]
        )
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        result = resolve_publish_version(ctx, "a.x", "2026-06-01")
        assert result == "2026-06-01"

    def test_step2_configured_version_wins_over_completed(self, tmp_path: Path) -> None:
        """Test Step2 configured version wins over completed."""
        lock = _write_versioned_lock(
            tmp_path, configured_version="2026-01-01", completed_versions=["2025-12-01"]
        )
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        result = resolve_publish_version(ctx, "a.x", None)
        assert result == "2026-01-01"

    def test_step3_latest_completed_as_fallback(self, tmp_path: Path) -> None:
        """Test Step3 latest completed as fallback."""
        lock = _write_versioned_lock(tmp_path, completed_versions=["2025-11-01", "2025-12-01"])
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        result = resolve_publish_version(ctx, "a.x", None)
        assert result == "2025-12-01"

    def test_raises_when_no_version_available(self, tmp_path: Path) -> None:
        """Test Raises when no version available."""
        lock = _write_versioned_lock(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        with pytest.raises(RuntimeError, match="No version available"):
            resolve_publish_version(ctx, "a.x", None)


# ---------------------------------------------------------------------------
# Version resolution for ``dbp model publish``
# ---------------------------------------------------------------------------


class TestResolvePublishVersionForPublish:
    """Lock the 2-step version resolution for ``dbp model publish``."""

    def test_step1_explicit_version_wins(self, tmp_path: Path) -> None:
        """Test Step1 explicit version wins."""
        lock = _write_versioned_lock(
            tmp_path, configured_version="2026-01-01", completed_versions=["2025-12-01"]
        )
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        result = resolve_publish_version_for_publish(ctx, "a.x", "2026-06-01")
        assert result == "2026-06-01"

    def test_step2_latest_completed_version(self, tmp_path: Path) -> None:
        """Test Step2 latest completed version."""
        lock = _write_versioned_lock(tmp_path, completed_versions=["2025-11-01", "2025-12-01"])
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        result = resolve_publish_version_for_publish(ctx, "a.x", None)
        assert result == "2025-12-01"

    def test_ignores_configured_version(self, tmp_path: Path) -> None:
        """Configured version is intentionally skipped for publish-only."""
        lock = _write_versioned_lock(tmp_path, configured_version="2026-01-01")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        with pytest.raises(RuntimeError, match="No completed versions"):
            resolve_publish_version_for_publish(ctx, "a.x", None)

    def test_raises_when_no_completed_versions(self, tmp_path: Path) -> None:
        """Test Raises when no completed versions."""
        lock = _write_versioned_lock(tmp_path)
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        with pytest.raises(RuntimeError, match="No completed versions"):
            resolve_publish_version_for_publish(ctx, "a.x", None)


# ---------------------------------------------------------------------------
# Version resolution: intentional difference between run and publish
# ---------------------------------------------------------------------------


class TestRunVsPublishVersionDifference:
    """Document and test the intentional split between ``run`` and ``publish``.

    ``run`` (full lifecycle) uses the configured version because it is creating
    a new publish from scratch. ``publish`` (standalone) re-publishes existing
    data and defaults to the most recent completed version.
    """

    def test_run_uses_configured_version(self, tmp_path: Path) -> None:
        """Test Run uses configured version."""
        lock = _write_versioned_lock(tmp_path, configured_version="2026-03-17")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        result = resolve_publish_version(ctx, "a.x", None)
        assert result == "2026-03-17"

    def test_publish_ignores_configured_version(self, tmp_path: Path) -> None:
        """Test Publish ignores configured version."""
        lock = _write_versioned_lock(tmp_path, configured_version="2026-03-17")
        ctx = CliContext(project_path=tmp_path, lockfile_path=lock)
        with pytest.raises(RuntimeError):
            resolve_publish_version_for_publish(ctx, "a.x", None)


# ---------------------------------------------------------------------------
# Publish mode mapping
# ---------------------------------------------------------------------------


class TestResolvePublishMode:
    """Tests for TestResolvePublishMode."""

    def test_default_returns_none(self) -> None:
        """Test Default returns none."""
        assert resolve_publish_mode(dry_run=False, refresh=False) is None

    def test_dry_run(self) -> None:
        """Test Dry run."""
        assert resolve_publish_mode(dry_run=True, refresh=False) == "dry"

    def test_refresh(self) -> None:
        """Test Refresh."""
        assert resolve_publish_mode(dry_run=False, refresh=True) == "refresh"

    def test_dry_run_takes_precedence(self) -> None:
        """Test Dry run takes precedence."""
        assert resolve_publish_mode(dry_run=True, refresh=True) == "dry"
