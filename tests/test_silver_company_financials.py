from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from app.silver.silver_company_financials import transform_company_financials


FIN_SCHEMA = StructType([
    StructField("ticker", StringType(), True),
    StructField("cik", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("sec_metric", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("fiscal_year", IntegerType(), True),
    StructField("fiscal_period", StringType(), True),
    StructField("form_type", StringType(), True),
    StructField("filed_date", StringType(), True),
    StructField("frame", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


class TestTransformCompanyFinancials:
    def test_filters_non_target_forms(self, spark):
        rows = [
            ("AAPL", "0000320193", "revenue", "Revenues", "USD", 100.0, 2024, "FY", "10-K", "2024-10-30", None, "sec_edgar", "2026-04-12T02:40:47"),
            ("AAPL", "0000320193", "revenue", "Revenues", "USD", 100.0, 2024, "FY", "8-K", "2024-10-30", None, "sec_edgar", "2026-04-12T02:40:47"),
        ]
        df = spark.createDataFrame(rows, FIN_SCHEMA)
        out = transform_company_financials(df)
        assert out.count() == 1
        assert out.collect()[0].form_type == "10-K"

    def test_dedup_keeps_one_record(self, spark):
        rows = [
            ("MSFT", "0000789019", "assets", "Assets", "USD", 200.0, 2024, "FY", "10-K", "2024-07-30", None, "sec_edgar", "2026-04-12T02:40:47"),
            ("MSFT", "0000789019", "assets", "Assets", "USD", 200.0, 2024, "FY", "10-K", "2024-07-30", None, "sec_edgar", "2026-04-12T02:40:47"),
        ]
        df = spark.createDataFrame(rows, FIN_SCHEMA)
        out = transform_company_financials(df)
        assert out.count() == 1

    def test_dates_cast_and_audit_added(self, spark):
        rows = [
            ("NVDA", "0001045810", "eps_basic", "EarningsPerShareBasic", "USD/shares", 1.45, 2024, "FY", "10-K", "2024-03-18", None, "sec_edgar", "2026-04-12T02:40:47"),
        ]
        df = spark.createDataFrame(rows, FIN_SCHEMA)
        out = transform_company_financials(df)
        row = out.collect()[0]
        assert row.filed_date is not None
        assert "_silver_loaded_at" in out.columns
