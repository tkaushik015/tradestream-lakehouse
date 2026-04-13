"""
Shared SparkSession builder for Silver-layer transformations.
Configures Delta Lake, S3A (MinIO), and common Spark settings.
"""

import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "tradestream-bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "tradestream-silver")

BRONZE_PATH = f"s3a://{BRONZE_BUCKET}"
SILVER_PATH = f"s3a://{SILVER_BUCKET}"


def get_spark_session(app_name: str = "tradestream_silver") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )
    return builder.getOrCreate()
