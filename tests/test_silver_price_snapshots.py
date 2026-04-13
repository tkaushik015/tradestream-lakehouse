from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from app.silver.silver_price_snapshots import transform_price_snapshots


PRICE_SCHEMA = StructType([
    StructField("ticker_symbol", StringType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("event_ts", LongType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("source", StringType(), True),
])


class TestTransformPriceSnapshots:
    def test_null_ticker_filtered_out(self, spark):
        rows = [(None, 100.0, 1710000000000, None, None, None, None, None)]
        df = spark.createDataFrame(rows, PRICE_SCHEMA)
        out = transform_price_snapshots(df)
        assert out.count() == 0

    def test_zero_price_filtered_out(self, spark):
        rows = [("AAPL", 0.0, 1710000000000, None, None, None, None, None)]
        df = spark.createDataFrame(rows, PRICE_SCHEMA)
        out = transform_price_snapshots(df)
        assert out.count() == 0

    def test_negative_price_filtered_out(self, spark):
        rows = [("AAPL", -5.0, 1710000000000, None, None, None, None, None)]
        df = spark.createDataFrame(rows, PRICE_SCHEMA)
        out = transform_price_snapshots(df)
        assert out.count() == 0

    def test_ticker_uppercased(self, spark):
        rows = [(" aapl ", 100.0, 1710000000000, None, None, None, None, None)]
        df = spark.createDataFrame(rows, PRICE_SCHEMA)
        out = transform_price_snapshots(df)
        assert out.collect()[0].ticker == "AAPL"

    def test_trade_date_derived(self, spark):
        rows = [("AAPL", 100.0, 1710000000000, None, None, None, None, None)]
        df = spark.createDataFrame(rows, PRICE_SCHEMA)
        out = transform_price_snapshots(df)
        assert out.collect()[0].trade_date is not None

    def test_dedup_on_ticker_timestamp(self, spark):
        rows = [
            ("AAPL", 100.0, 1710000000000, None, None, None, None, None),
            ("AAPL", 100.0, 1710000000000, None, None, None, None, None),
        ]
        df = spark.createDataFrame(rows, PRICE_SCHEMA)
        out = transform_price_snapshots(df)
        assert out.count() == 1

    def test_renames_market_columns(self, spark):
        rows = [("MSFT", 300.0, 1710000000000, 305.0, 295.0, 299.0, 1000.0, "yfinance")]
        df = spark.createDataFrame(rows, PRICE_SCHEMA)
        out = transform_price_snapshots(df)
        row = out.collect()[0]
        assert row.price == 300.0
        assert row.high == 305.0
        assert row.low == 295.0
        assert row.open == 299.0

    def test_audit_column_present(self, spark):
        rows = [("NVDA", 800.0, 1710000000000, None, None, None, None, None)]
        df = spark.createDataFrame(rows, PRICE_SCHEMA)
        out = transform_price_snapshots(df)
        assert "_silver_loaded_at" in out.columns
