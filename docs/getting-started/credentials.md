# Credentials

DBPort connects to an Iceberg REST catalog backed by S3-compatible object storage. Credentials are resolved from two sources, in order:

1. **Constructor keyword arguments** passed to `DBPort(...)`
2. **Environment variables**

## Required credentials

| Environment variable | Constructor argument | Description |
|---|---|---|
| `ICEBERG_REST_URI` | `catalog_uri` | Iceberg REST catalog URL |
| `ICEBERG_CATALOG_TOKEN` | `catalog_token` | Bearer token for catalog authentication |
| `ICEBERG_WAREHOUSE` | `warehouse` | Warehouse name in the catalog |

All three are required. Missing credentials raise a clear error at startup.

## Optional credentials

| Environment variable | Constructor argument | Description |
|---|---|---|
| `S3_ENDPOINT` | `s3_endpoint` | S3-compatible object store endpoint |
| `AWS_ACCESS_KEY_ID` | `s3_access_key` | S3 access key ID |
| `AWS_SECRET_ACCESS_KEY` | `s3_secret_key` | S3 secret access key |

## Setting environment variables

Create a `.env` file (git-ignored) or export directly:

```bash
export ICEBERG_REST_URI="https://catalog.example.com"
export ICEBERG_CATALOG_TOKEN="your-token-here"
export ICEBERG_WAREHOUSE="my_warehouse"
```

## Security

**Credentials are never written to disk.** The `dbport.lock` file is credential-free and safe to commit. Constructor arguments take precedence over environment variables, so you can override credentials per-run without changing your environment.

## Health check

Verify your credentials are configured correctly:

```bash
dbp status check
```

This checks that all required environment variables are present, DuckDB is available, and dependencies are installed.
