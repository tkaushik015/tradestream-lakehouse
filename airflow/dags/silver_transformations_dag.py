from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ENV = {
    "PYSPARK_PYTHON": "/opt/anaconda3/bin/python",
    "PYSPARK_DRIVER_PYTHON": "/opt/anaconda3/bin/python",
    "JAVA_HOME": "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "PATH": "/usr/local/opt/openjdk@17/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/anaconda3/bin",
    "JDK_JAVA_OPTIONS": "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "JAVA_TOOL_OPTIONS": "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
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
        /opt/anaconda3/bin/python -m app.silver.silver_trade_orders
        """,
        env=DEFAULT_ENV,
    )

    silver_price_snapshots = BashOperator(
        task_id="silver_price_snapshots",
        bash_command="""
        cd /opt/tradestream && \
        /opt/anaconda3/bin/python -m app.silver.silver_price_snapshots
        """,
        env=DEFAULT_ENV,
    )

    silver_company_financials = BashOperator(
        task_id="silver_company_financials",
        bash_command="""
        cd /opt/tradestream && \
        /opt/anaconda3/bin/python -m app.silver.silver_company_financials
        """,
        env=DEFAULT_ENV,
    )

    silver_trade_orders >> silver_price_snapshots >> silver_company_financials
