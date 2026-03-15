"""WarehouseCreds — credential resolution via pydantic-settings.

Credentials are resolved in order (highest priority first):
1. Explicit kwargs passed to DBPort(...)
2. .env file (project-local)
3. Environment variables (ICEBERG_*, S3_*, AWS_*)

No secrets are written to disk (dbport.lock is credential-free).
"""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WarehouseCreds(BaseSettings):
    """All credentials needed to connect to the Iceberg REST catalog and S3 object store."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    catalog_uri: str = Field(
        ...,
        validation_alias=AliasChoices("ICEBERG_REST_URI", "iceberg_rest_uri"),
        description="Iceberg REST catalog URL",
    )
    catalog_token: str = Field(
        ...,
        validation_alias=AliasChoices("ICEBERG_CATALOG_TOKEN", "iceberg_catalog_token"),
        description="Bearer token for catalog auth",
    )
    warehouse: str = Field(
        ...,
        validation_alias=AliasChoices("ICEBERG_WAREHOUSE", "iceberg_warehouse"),
        description="Warehouse name in the catalog",
    )

    s3_endpoint: str | None = Field(
        None,
        validation_alias=AliasChoices("S3_ENDPOINT", "s3_endpoint"),
        description="S3-compatible object store endpoint",
    )
    s3_access_key: str | None = Field(
        None,
        validation_alias=AliasChoices("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
        description="S3 access key ID",
    )
    s3_secret_key: str | None = Field(
        None,
        validation_alias=AliasChoices("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
        description="S3 secret access key",
    )
    s3_region: str = Field(
        "us-east-1",
        validation_alias=AliasChoices("AWS_DEFAULT_REGION", "aws_default_region"),
        description="S3 region",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        **kwargs: Any,
    ) -> tuple:
        """Override source priority: init > .env file > shell env vars."""
        return (
            kwargs["init_settings"],
            kwargs["dotenv_settings"],
            kwargs["env_settings"],
            kwargs["file_secret_settings"],
        )
