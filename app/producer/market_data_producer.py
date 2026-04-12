import json
import time
from datetime import datetime

import pandas as pd
import yfinance as yf
from confluent_kafka import Producer


TICKERS = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL",
    "META", "TSLA", "JPM", "V", "XOM"
]

KAFKA_TOPIC = "market.price_updates"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": "localhost:9092"})


def fetch_latest_prices(tickers: list[str]) -> list[dict]:
    data = yf.download(
        tickers=tickers,
        period="1d",
        interval="5m",
        group_by="ticker",
        progress=False,
        auto_adjust=False,
        threads=True,
    )

    records: list[dict] = []

    for ticker in tickers:
        try:
            if ticker not in data:
                continue

            ticker_df = data[ticker].dropna()
            if ticker_df.empty:
                continue

            latest = ticker_df.iloc[-1]

            record = {
                "ticker_symbol": ticker,
                "open_price": float(latest["Open"]),
                "high_price": float(latest["High"]),
                "low_price": float(latest["Low"]),
                "close_price": float(latest["Close"]),
                "volume": int(latest["Volume"]),
                "event_ts": int(time.time() * 1000),
                "market_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "source": "yfinance",
            }
            records.append(record)
        except Exception as exc:
            print(f"Error building record for {ticker}: {exc}")

    return records


def publish_once():
    producer = build_producer()
    records = fetch_latest_prices(TICKERS)

    sent = 0
    for record in records:
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=record["ticker_symbol"],
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report,
            )
            sent += 1
        except Exception as exc:
            print(f"Error publishing {record['ticker_symbol']}: {exc}")

    producer.flush()
    print(f"Published {sent} price events to Kafka")


if __name__ == "__main__":
    publish_once()