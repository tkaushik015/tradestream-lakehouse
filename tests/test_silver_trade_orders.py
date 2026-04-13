from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)
from app.silver.silver_trade_orders import transform_trade_orders


TRADE_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("limit_price", StringType(), True),
    StructField("executed_price", StringType(), True),
    StructField("status", StringType(), True),
    StructField("placed_at", LongType(), True),
    StructField("executed_at", LongType(), True),
    StructField("settled_at", LongType(), True),
    StructField("updated_at", LongType(), True),
    StructField("_cdc_op", StringType(), True),
    StructField("_cdc_ts_ms", LongType(), True),
])


class TestTransformTradeOrders:
    def test_dedup_keeps_latest_per_order(self, spark):
        rows = [
            ("o1", "a1", " aapl ", "LIMIT", "10", "100.5", "101.2", "OPEN",
             1710000000000000, None, None, 1710000000000000, "u", 1000),
            ("o1", "a1", "aapl", "LIMIT", "10", "100.5", "101.2", "FILLED",
             1710000000000000, 1710001000000000, None, 1710001000000000, "u", 2000),
        ]
        df = spark.createDataFrame(rows, TRADE_SCHEMA)
        out = transform_trade_orders(df)

        assert out.count() == 1
        row = out.collect()[0]
        assert row.order_id == "o1"
        assert row.status == "filled"

    def test_ticker_uppercased_and_trimmed(self, spark):
        rows = [
            ("o2", "a2", " msft ", "market", "5", None, "250.1", "OPEN",
             1710000000000000, None, None, 1710000000000000, "u", 1000),
        ]
        df = spark.createDataFrame(rows, TRADE_SCHEMA)
        out = transform_trade_orders(df)
        assert out.collect()[0].ticker == "MSFT"

    def test_order_type_and_status_lowercased(self, spark):
        rows = [
            ("o3", "a3", "NVDA", "LIMIT", "2", "900.0", None, "OPEN",
             1710000000000000, None, None, 1710000000000000, "u", 1000),
        ]
        df = spark.createDataFrame(rows, TRADE_SCHEMA)
        out = transform_trade_orders(df)
        row = out.collect()[0]
        assert row.order_type == "limit"
        assert row.status == "open"

    def test_audit_columns_added(self, spark):
        rows = [
            ("o4", "a4", "AMZN", "market", "3", None, "180.0", "FILLED",
             1710000000000000, 1710000100000000, None, 1710000100000000, "u", 1000),
        ]
        df = spark.createDataFrame(rows, TRADE_SCHEMA)
        out = transform_trade_orders(df)
        row = out.collect()[0]
        assert "_silver_loaded_at" in out.columns
        assert row._cdc_operation == "u"

    def test_multiple_orders_preserved(self, spark):
        rows = [
            ("o5", "a5", "AAPL", "market", "1", None, "100.0", "filled",
             1710000000000000, 1710000100000000, None, 1710000100000000, "u", 1000),
            ("o6", "a6", "MSFT", "limit", "2", "200.0", None, "open",
             1710000000000000, None, None, 1710000100000000, "u", 1000),
        ]
        df = spark.createDataFrame(rows, TRADE_SCHEMA)
        out = transform_trade_orders(df)
        assert out.count() == 2

    def test_quantity_cast_to_int(self, spark):
        rows = [
            ("o7", "a7", "GOOGL", "limit", "7", "150.0", None, "open",
             1710000000000000, None, None, 1710000100000000, "u", 1000),
        ]
        df = spark.createDataFrame(rows, TRADE_SCHEMA)
        out = transform_trade_orders(df)
        row = out.collect()[0]
        assert isinstance(row.quantity, int)
        assert row.quantity == 7
