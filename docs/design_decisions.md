# Design Decisions

## Why PostgreSQL?
Used as OLTP source system for CDC simulation.

## Why Debezium?
Used to capture row-level database changes without custom polling logic.

## Why Kafka?
Provides event streaming and decouples ingestion from downstream consumers.

## Why dbt?
Used for modular SQL transformations and dimensional modeling.

## Why Great Expectations?
Used to formalize and automate data quality checks.