import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.silver.spark_config import SILVER_PATH, get_spark_session


def main():
    spark = get_spark_session("data_quality_silver")

    try:
        trade_orders = spark.read.format("delta").load(f"{SILVER_PATH}/trade_orders")
        price_snapshots = spark.read.format("delta").load(f"{SILVER_PATH}/price_snapshots")
        company_financials = spark.read.format("delta").load(f"{SILVER_PATH}/company_financials")

        print("trade_orders count:", trade_orders.count())
        print("price_snapshots count:", price_snapshots.count())
        print("company_financials count:", company_financials.count())

        assert trade_orders.count() > 0, "trade_orders is empty"
        assert price_snapshots.count() > 0, "price_snapshots is empty"
        assert company_financials.count() > 0, "company_financials is empty"

        assert "order_id" in trade_orders.columns
        assert "ticker" in trade_orders.columns

        assert "ticker" in price_snapshots.columns
        assert "price" in price_snapshots.columns

        assert "ticker" in company_financials.columns
        assert "metric" in company_financials.columns
        assert "value" in company_financials.columns

        print("Silver data quality checks passed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
