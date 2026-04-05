# Architecture Notes

TradeStream is designed as a production-style lakehouse project.

Main flow:
1. Trade and company data originates in PostgreSQL
2. CDC changes are captured using Debezium
3. Events are published into Kafka
4. Streaming or batch jobs load Bronze and Silver layers
5. dbt builds marts and dimensional models
6. Great Expectations validates data quality
7. Airflow orchestrates workflows