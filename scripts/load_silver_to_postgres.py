from pyspark.sql import SparkSession
from app.silver.spark_config import SILVER_PATH

POSTGRES_URL = "jdbc:postgresql://localhost:5432/trading_db"
POSTGRES_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

spark = (
    SparkSession.builder
    .appName("load_silver_to_postgres")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.2.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.7.3",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

try:
    price_df = spark.read.format("delta").load(f"{SILVER_PATH}/price_snapshots")
    company_df = spark.read.format("delta").load(f"{SILVER_PATH}/company_financials")

    price_df.write.mode("overwrite").jdbc(
        url=POSTGRES_URL,
        table="public.price_snapshots",
        properties=POSTGRES_PROPS,
    )

    company_df.write.mode("overwrite").jdbc(
        url=POSTGRES_URL,
        table="public.company_financials",
        properties=POSTGRES_PROPS,
    )

    print("Loaded price_snapshots and company_financials into Postgres.")
finally:
    spark.stop()
