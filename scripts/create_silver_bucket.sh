#!/usr/bin/env bash
# scripts/create_silver_bucket.sh
# Creates the tradestream-silver bucket in MinIO if it doesn't exist.
#
# Usage:  bash scripts/create_silver_bucket.sh

set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
BUCKET="tradestream-silver"

echo "--- Configuring mc alias ---"
mc alias set local "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api S3v4

if mc ls "local/$BUCKET" > /dev/null 2>&1; then
    echo "Bucket '$BUCKET' already exists."
else
    mc mb "local/$BUCKET"
    echo "Bucket '$BUCKET' created."
fi

echo "--- Current buckets ---"
mc ls local/
