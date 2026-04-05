# TradeStream Lakehouse

An end-to-end real-time market data and trade analytics platform built with **PostgreSQL CDC, Debezium, Kafka, Spark/Databricks, Delta Lake, dbt, Great Expectations, and Airflow**.

TradeStream simulates a production-grade lakehouse pipeline for ingesting trade orders, market prices, and company fundamentals into analytics-ready datasets for downstream reporting and decision-making.

---

## Architecture

```text
                    +----------------------+
                    |   Trading App /      |
                    |   Market Simulator   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    PostgreSQL OLTP   |
                    | trades, companies    |
                    +----------+-----------+
                               |
                      CDC via WAL / Debezium
                               |
                               v
                    +----------------------+
                    |       Kafka          |
                    | trade_events         |
                    | price_events         |
                    | sec_filings          |
                    +----------+-----------+
                               |
                               v
               +--------------------------------------+
               | Spark Structured Streaming / DLT     |
               | Bronze -> Silver transformations     |
               +------------------+-------------------+
                                  |
                                  v
                        +----------------------+
                        |   Delta Lake / S3    |
                        | bronze silver gold   |
                        +----------+-----------+
                                   |
                        +----------+-----------+
                        |                      |
                        v                      v
              +------------------+    +----------------------+
              | Great Expectations|    |        dbt          |
              | data validation   |    | marts + SCD2 dims   |
              +------------------+    +----------+-----------+
                                                 |
                                                 v
                                       +----------------------+
                                       | BI / Analytics Layer |
                                       | dashboards / reports |
                                       +----------------------+