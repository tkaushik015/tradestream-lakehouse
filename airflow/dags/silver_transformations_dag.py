from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ENV = {
    "PYSPARK_PYTHON": "python",
    "PYSPARK_DRIVER_PYTHON": "python",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
    "PATH": "/usr/lib/jvm/java-17-openjdk-arm64/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
    "PYTHONPATH": "/opt/tradestream",
}

with DAG(
    dag_id="silver_transformations_dag",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["tradestream", "silver", "spark"],
) as dag:

    silver_trade_orders = BashOperator(
        task_id="silver_trade_orders",
        bash_command="""
        cd /opt/tradestream && \
        python -m app.silver.silver_trade_orders
        """,
        env=DEFAULT_ENV,
    )

    silver_price_snapshots = BashOperator(
        task_id="silver_price_snapshots",
        bash_command="""
        cd /opt/tradestream && \
        python -m app.silver.silver_price_snapshots
        """,
        env=DEFAULT_ENV,
    )

    silver_company_financials = BashOperator(
        task_id="silver_company_financials",
        bash_command="""
        cd /opt/tradestream && \
        python -m app.silver.silver_company_financials
        """,
        env=DEFAULT_ENV,
    )

    silver_trade_orders >> silver_price_snapshots >> silver_company_financials
