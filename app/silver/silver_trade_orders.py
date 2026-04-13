"""
Silver Trade Orders Transformation
Reads Parquet Bronze trade order data from MinIO and writes Delta Silver output.
Matches the actual Bronze schema produced by your CDC sink.
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from pyspark.sql.window import Window

from app.silver.spark_config import BRONZE_PATH, SILVER_PATH, get_spark_session

BRONZE_TRADE_ORDERS = f"{BRONZE_PATH}/topics/trading.public.trade_orders/"
SILVER_TRADE_ORDERS = f"{SILVER_PATH}/trade_orders"


def read_bronze_trade_orders(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.parquet(path)

    rename_candidates = {
        "ticker_symbol": "ticker",
        "__op": "_cdc_op",
        "__ts_ms": "_cdc_ts_ms",
    }

    existing = set(df.columns)
    for old, new in rename_candidates.items():
        if old in existing and new not in existing:
            df = df.withColumnRenamed(old, new)

    if "_cdc_op" not in df.columns:
        df = df.withColumn("_cdc_op", F.lit("r"))

    if "_cdc_ts_ms" not in df.columns:
        df = df.withColumn("_cdc_ts_ms", F.lit(0).cast("long"))

    df = df.filter(~F.col("_cdc_op").isin("d"))
    return df


def parse_timestamp_column(df: DataFrame, col_name: str) -> DataFrame:
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

def transform_trade_orders(df: DataFrame) -> DataFrame:
    for ts_col in ("placed_at", "executed_at", "settled_at", "updated_at"):
        df = parse_timestamp_column(df, ts_col)

    sort_expr = (
        F.col("_cdc_ts_ms").cast("long").desc_nulls_last()
        if "_cdc_ts_ms" in df.columns
        else F.col("updated_at").desc_nulls_last()
    )

    w = Window.partitionBy("order_id").orderBy(sort_expr, F.col("updated_at").desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    df = (
        df
        .withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("account_id", F.col("account_id").cast(StringType()))
        .withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
        .withColumn("order_type", F.lower(F.trim(F.col("order_type"))))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("limit_price", F.col("limit_price").cast("string").cast(DoubleType()))
        .withColumn("executed_price", F.col("executed_price").cast("string").cast(DoubleType()))
    )

    run_ts = datetime.now(timezone.utc).isoformat()
    df = (
        df
        .withColumn("_silver_loaded_at", F.lit(run_ts).cast(TimestampType()))
        .withColumn("_cdc_operation", F.col("_cdc_op"))
        .drop("_cdc_op")
    )

    desired = [
        "order_id",
        "account_id",
        "ticker",
        "order_type",
        "quantity",
        "limit_price",
        "executed_price",
        "status",
        "placed_at",
        "executed_at",
        "settled_at",
        "updated_at",
        "_cdc_operation",
        "_silver_loaded_at",
    ]
    final_cols = [c for c in desired if c in df.columns]
    return df.select(*final_cols)


def write_silver(df: DataFrame, target_path: str) -> None:
    df.write.format("delta").mode("overwrite").save(target_path)
    print(f"[silver_trade_orders] Wrote {df.count()} rows to {target_path}")


def run():
    print("=" * 60)
    print("[silver_trade_orders] Starting Silver transformation")
    print("=" * 60)

    spark = get_spark_session("silver_trade_orders")

    try:
        raw = read_bronze_trade_orders(spark, BRONZE_TRADE_ORDERS)
        print(f"[silver_trade_orders] Bronze rows read: {raw.count()}")

        cleaned = transform_trade_orders(raw)
        print(f"[silver_trade_orders] Rows after transform: {cleaned.count()}")
        cleaned.printSchema()
        cleaned.show(10, truncate=False)

        write_silver(cleaned, SILVER_TRADE_ORDERS)
    finally:
        spark.stop()

    print("[silver_trade_orders] Done.")


if __name__ == "__main__":
    run()
