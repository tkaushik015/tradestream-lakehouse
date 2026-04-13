from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "tushar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_data_producer_dag",
    default_args=default_args,
    description="Runs market data producer and publishes price updates to Kafka",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tradestream", "market", "bronze"],
) as dag:

    run_market_producer = BashOperator(
        task_id="run_market_data_producer",
        bash_command="""
        cd /opt/tradestream && \
        pip install yfinance confluent-kafka pandas requests && \
        python app/producer/market_data_producer.py
        """,
    )

    run_market_producer