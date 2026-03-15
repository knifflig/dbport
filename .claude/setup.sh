#!/usr/bin/env bash
set -euo pipefail

REQUIRED_VARS=(
  ICEBERG_REST_URI
  ICEBERG_CATALOG_TOKEN
  ICEBERG_WAREHOUSE
  S3_ENDPOINT
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
)

missing=()
for var in "${REQUIRED_VARS[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    missing+=("$var")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Error: missing required environment variables:" >&2
  for var in "${missing[@]}"; do
    echo "  - $var" >&2
  done
  exit 1
fi

ENV_FILE="$(dirname "$0")/.env"

{
  for var in "${REQUIRED_VARS[@]}"; do
    echo "${var}=${!var}"
  done
} > "$ENV_FILE"

echo "Written to $ENV_FILE"
