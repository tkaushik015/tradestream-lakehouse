"""
Silver Price Snapshots Transformation
Reads Bronze market JSON from MinIO and writes Delta Silver output.
Matches the actual Bronze market schema in this project.
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType

from app.silver.spark_config import BRONZE_PATH, SILVER_PATH, get_spark_session

BRONZE_MARKET = f"{BRONZE_PATH}/topics/market.price_updates/"
SILVER_PRICE_SNAPSHOTS = f"{SILVER_PATH}/price_snapshots"


def read_bronze_market(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.option("multiline", "false").json(path)
    return df


def parse_event_ts(df: DataFrame, col_name: str) -> DataFrame:
    if col_name not in df.columns:
        return df

    col_as_str = F.col(col_name).cast("string")
    col_as_long = F.col(col_name).cast("long")

    return df.withColumn(
        col_name,
        F.when(
            col_as_long.isNotNull() & (F.length(col_as_str) >= 16),
            F.to_timestamp(F.from_unixtime(col_as_long.cast("double") / 1_000_000.0))
        ).when(
            col_as_long.isNotNull() & (F.length(col_as_str).between(13, 15)),
            F.to_timestamp(F.from_unixtime(col_as_long.cast("double") / 1_000.0))
        ).when(
            col_as_long.isNotNull() & (F.length(col_as_str) <= 10),
            F.to_timestamp(F.from_unixtime(col_as_long.cast("double")))
        ).otherwise(
            F.to_timestamp(F.col(col_name))
        )
    )


def transform_price_snapshots(df: DataFrame) -> DataFrame:
    rename_map = {
        "ticker_symbol": "ticker",
        "close_price": "price",
        "event_ts": "event_timestamp",
        "high_price": "high",
        "low_price": "low",
        "open_price": "open",
    }

    existing = set(df.columns)
    for old, new in rename_map.items():
        if old in existing and new not in existing:
            df = df.withColumnRenamed(old, new)

    df = df.withColumn("ticker", F.trim(F.upper(F.col("ticker").cast(StringType()))))

    for num_col in ("price", "volume", "high", "low", "open"):
        if num_col in df.columns:
            df = df.withColumn(num_col, F.col(num_col).cast(DoubleType()))

    df = parse_event_ts(df, "event_timestamp")

    if "event_timestamp" not in df.columns:
        df = df.withColumn("event_timestamp", F.current_timestamp())

    df = df.withColumn("trade_date", F.to_date(F.col("event_timestamp")))

    df = df.filter(
        (F.col("ticker").isNotNull())
        & (F.col("ticker") != "")
        & (F.col("price").isNotNull())
        & (F.col("price") > 0)
    )

    df = df.dropDuplicates(["ticker", "event_timestamp"])

    run_ts = datetime.now(timezone.utc).isoformat()
    df = df.withColumn("_silver_loaded_at", F.lit(run_ts).cast(TimestampType()))

    desired = [
        "ticker",
        "price",
        "volume",
        "high",
        "low",
        "open",
        "source",
        "event_timestamp",
        "trade_date",
        "_silver_loaded_at",
    ]
    final_cols = [c for c in desired if c in df.columns]
    return df.select(*final_cols)


def write_to_silver(df: DataFrame, target_path: str):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("trade_date")
        .save(target_path)
    )
    print(f"[silver_price_snapshots] Wrote {df.count()} rows to {target_path}")


def run():
    print("=" * 60)
    print("[silver_price_snapshots] Starting Silver transformation")
    print("=" * 60)

    spark = get_spark_session("silver_price_snapshots")

    try:
        raw = read_bronze_market(spark, BRONZE_MARKET)
        print(f"[silver_price_snapshots] Bronze records read: {raw.count()}")

        cleaned = transform_price_snapshots(raw)
        print(f"[silver_price_snapshots] Records after transform: {cleaned.count()}")
        cleaned.printSchema()
        cleaned.show(10, truncate=False)

        write_to_silver(cleaned, SILVER_PRICE_SNAPSHOTS)
    finally:
        spark.stop()

    print("[silver_price_snapshots] Done.")


if __name__ == "__main__":
    run()
