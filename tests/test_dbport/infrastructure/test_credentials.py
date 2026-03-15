"""Tests for infrastructure.credentials."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from pydantic_settings import BaseSettings

from dbport.infrastructure.credentials import WarehouseCreds


class TestWarehouseCreds:
    def test_construction_explicit(self):
        creds = WarehouseCreds(
            catalog_uri="https://catalog.example.com",
            catalog_token="tok-secret",
            warehouse="my_warehouse",
        )
        assert creds.catalog_uri == "https://catalog.example.com"
        assert creds.catalog_token == "tok-secret"
        assert creds.warehouse == "my_warehouse"

    def test_optional_s3_fields_default_none(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)  # escape repo-root .env file
        for var in ("S3_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            monkeypatch.delenv(var, raising=False)
        creds = WarehouseCreds(
            catalog_uri="https://catalog.example.com",
            catalog_token="tok",
            warehouse="wh",
        )
        assert creds.s3_endpoint is None
        assert creds.s3_access_key is None
        assert creds.s3_secret_key is None

    def test_s3_region_default(self):
        creds = WarehouseCreds(
            catalog_uri="https://catalog.example.com",
            catalog_token="tok",
            warehouse="wh",
        )
        assert creds.s3_region == "us-east-1"

    def test_s3_endpoint_set_explicit(self):
        creds = WarehouseCreds(
            catalog_uri="https://catalog.example.com",
            catalog_token="tok",
            warehouse="wh",
            s3_endpoint="https://s3.example.com",
        )
        assert creds.s3_endpoint == "https://s3.example.com"

    def test_s3_access_key_field_exists(self):
        assert "s3_access_key" in WarehouseCreds.model_fields
        # No alias — env var must be AWS_ACCESS_KEY_ID (not AWS_ACCESS_KEY_ID_ID)
        field_info = WarehouseCreds.model_fields["s3_access_key"]
        assert field_info.alias is None

    def test_s3_secret_key_field_exists(self):
        assert "s3_secret_key" in WarehouseCreds.model_fields
        # Field has validation_alias for AWS_SECRET_ACCESS_KEY
        field_info = WarehouseCreds.model_fields["s3_secret_key"]
        assert field_info.alias is None

    def test_s3_access_key_from_env(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_REST_URI", "https://catalog.example.com")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "tok")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "wh")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
        creds = WarehouseCreds()
        assert creds.s3_access_key == "AKIAIOSFODNN7EXAMPLE"

    def test_s3_secret_key_from_env(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_REST_URI", "https://catalog.example.com")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "tok")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "wh")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        creds = WarehouseCreds()
        assert creds.s3_secret_key == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

    def test_is_base_settings(self):
        assert issubclass(WarehouseCreds, BaseSettings)

    def test_populate_by_name(self):
        assert WarehouseCreds.model_config.get("populate_by_name") is True

    def test_from_env_vars(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_REST_URI", "https://env-catalog.example.com")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "env-token")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "env_warehouse")
        creds = WarehouseCreds()
        assert creds.catalog_uri == "https://env-catalog.example.com"
        assert creds.catalog_token == "env-token"
        assert creds.warehouse == "env_warehouse"

    def test_explicit_overrides_env(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_REST_URI", "https://env.example.com")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "env-token")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "env_wh")
        creds = WarehouseCreds(catalog_uri="https://explicit.example.com")
        assert creds.catalog_uri == "https://explicit.example.com"

    def test_missing_required_fields_raises(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)  # escape repo-root .env file
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
        with pytest.raises((ValidationError, Exception)):
            WarehouseCreds()

    def test_dotenv_overrides_env_var(self, monkeypatch, tmp_path):
        """A .env file should take priority over shell env vars."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("ICEBERG_REST_URI", "https://from-shell.example.com")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "shell-token")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "shell_wh")

        dotenv = tmp_path / ".env"
        dotenv.write_text(
            "ICEBERG_REST_URI=https://from-dotenv.example.com\n"
            "ICEBERG_CATALOG_TOKEN=dotenv-token\n"
            "ICEBERG_WAREHOUSE=dotenv_wh\n"
        )

        creds = WarehouseCreds()
        assert creds.catalog_uri == "https://from-dotenv.example.com"
        assert creds.catalog_token == "dotenv-token"
        assert creds.warehouse == "dotenv_wh"

    def test_explicit_overrides_dotenv(self, monkeypatch, tmp_path):
        """Constructor kwargs should take priority over .env file."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("ICEBERG_REST_URI", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)

        dotenv = tmp_path / ".env"
        dotenv.write_text(
            "ICEBERG_REST_URI=https://from-dotenv.example.com\n"
            "ICEBERG_CATALOG_TOKEN=dotenv-token\n"
            "ICEBERG_WAREHOUSE=dotenv_wh\n"
        )

        creds = WarehouseCreds(catalog_uri="https://explicit.example.com")
        assert creds.catalog_uri == "https://explicit.example.com"
        assert creds.catalog_token == "dotenv-token"  # from .env

    def test_env_var_fallback_when_no_dotenv(self, monkeypatch, tmp_path):
        """Shell env vars are used when no .env file is present."""
        monkeypatch.chdir(tmp_path)  # no .env here
        monkeypatch.setenv("ICEBERG_REST_URI", "https://from-shell.example.com")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "shell-token")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "shell_wh")

        creds = WarehouseCreds()
        assert creds.catalog_uri == "https://from-shell.example.com"
