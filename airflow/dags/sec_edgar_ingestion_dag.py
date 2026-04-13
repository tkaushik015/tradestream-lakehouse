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
    dag_id="sec_edgar_ingestion_dag",
    default_args=default_args,
    description="Runs SEC EDGAR ingestion and uploads newest raw JSON file to Bronze",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tradestream", "edgar", "bronze"],
) as dag:

    run_sec_edgar_ingestion = BashOperator(
        task_id="run_sec_edgar_ingestion",
        bash_command="""
        cd /opt/tradestream && \
        pip install requests && \
        python app/producer/sec_ingestion.py
        """,
    )

    upload_latest_edgar_to_bronze = BashOperator(
        task_id="upload_latest_edgar_to_bronze",
        bash_command="""
        cd /opt/tradestream && \
        pip install boto3 && \
        python - <<'PY'
import os
import glob
from datetime import datetime
import boto3

files = glob.glob("/opt/tradestream/data/raw/edgar/edgar_facts_*.json")
if not files:
    raise FileNotFoundError("No EDGAR files found in data/raw/edgar/")

latest_file = max(files, key=os.path.getmtime)
now = datetime.utcnow()
year = now.strftime("%Y")
month = now.strftime("%m")

bucket = "tradestream-bronze"
key = f"edgar_facts/year={year}/month={month}/{os.path.basename(latest_file)}"

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

s3.upload_file(latest_file, bucket, key)
print(f"Uploaded {latest_file} to s3://{bucket}/{key}")
PY
        """,
    )

    run_sec_edgar_ingestion >> upload_latest_edgar_to_bronze