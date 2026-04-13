"""
Silver Company Financials Transformation
Reads flattened EDGAR facts JSON from Bronze and writes Delta Silver output.
Matches the actual output shape of app/producer/sec_ingestion.py.
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

from app.silver.spark_config import BRONZE_PATH, SILVER_PATH, get_spark_session

BRONZE_EDGAR = f"{BRONZE_PATH}/edgar_facts/"
SILVER_COMPANY_FIN = f"{SILVER_PATH}/company_financials"

TARGET_FORMS = {"10-K", "10-Q"}


def read_bronze_edgar(spark: SparkSession, path: str) -> DataFrame:
    """
    Read flattened EDGAR JSON rows written from sec_ingestion.py.
    Bronze records are already flat, not raw nested SEC companyfacts JSON.
    """
    return spark.read.option("multiline", "true").json(path)


def transform_company_financials(df: DataFrame) -> DataFrame:
    rename_map = {
        "metric_name": "metric",
        "sec_metric_name": "sec_metric",
        "fy": "fiscal_year",
        "fp": "fiscal_period",
        "form": "form_type",
        "filed": "filed_date",
    }

    existing = set(df.columns)
    for old, new in rename_map.items():
        if old in existing and new not in existing:
            df = df.withColumnRenamed(old, new)

    df = (
        df
        .withColumn("ticker", F.upper(F.trim(F.col("ticker").cast(StringType()))))
        .withColumn("cik", F.col("cik").cast(StringType()))
        .withColumn("metric", F.col("metric").cast(StringType()))
        .withColumn("sec_metric", F.col("sec_metric").cast(StringType()))
        .withColumn("unit", F.col("unit").cast(StringType()))
        .withColumn("value", F.col("value").cast(DoubleType()))
        .withColumn("fiscal_year", F.col("fiscal_year").cast(IntegerType()))
        .withColumn("fiscal_period", F.col("fiscal_period").cast(StringType()))
        .withColumn("form_type", F.col("form_type").cast(StringType()))
        .withColumn("frame", F.col("frame").cast(StringType()))
        .withColumn("source", F.col("source").cast(StringType()))
    )

    if "filed_date" in df.columns:
        df = df.withColumn("filed_date", F.to_date(F.col("filed_date")))

    if "ingested_at" in df.columns:
        df = df.withColumn("ingested_at", F.to_timestamp(F.col("ingested_at")))
    else:
        df = df.withColumn("ingested_at", F.current_timestamp())

    df = df.filter(
        (F.col("ticker").isNotNull()) &
        (F.col("ticker") != "") &
        (F.col("metric").isNotNull()) &
        (F.col("value").isNotNull()) &
        (F.col("form_type").isin(*TARGET_FORMS))
    )

    df = df.dropDuplicates(["ticker", "metric", "fiscal_year", "fiscal_period", "form_type", "filed_date"])

    run_ts = datetime.now(timezone.utc).isoformat()
    df = df.withColumn("_silver_loaded_at", F.lit(run_ts).cast(TimestampType()))

    desired = [
        "ticker",
        "cik",
        "metric",
        "sec_metric",
        "unit",
        "value",
        "fiscal_year",
        "fiscal_period",
        "form_type",
        "filed_date",
        "frame",
        "source",
        "ingested_at",
        "_silver_loaded_at",
    ]
    final_cols = [c for c in desired if c in df.columns]
    return df.select(*final_cols)


def write_to_silver(df: DataFrame, target_path: str):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("ticker")
        .save(target_path)
    )
    print(f"[silver_company_financials] Wrote {df.count()} rows to {target_path}")


def run():
    print("=" * 60)
    print("[silver_company_financials] Starting Silver transformation")
    print("=" * 60)

    spark = get_spark_session("silver_company_financials")

    try:
        raw = read_bronze_edgar(spark, BRONZE_EDGAR)
        print(f"[silver_company_financials] Bronze rows read: {raw.count()}")

        cleaned = transform_company_financials(raw)
        print(f"[silver_company_financials] Rows after transform: {cleaned.count()}")
        cleaned.printSchema()
        cleaned.show(20, truncate=False)

        write_to_silver(cleaned, SILVER_COMPANY_FIN)
    finally:
        spark.stop()

    print("[silver_company_financials] Done.")


if __name__ == "__main__":
    run()
