import json
import os
from datetime import datetime

import requests

OUTPUT_DIR = "data/raw/edgar"
TICKERS = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL"]
USER_AGENT = "TradeStream tusharkaushik@example.com"


def fetch_ticker_cik_map():
    url = "https://www.sec.gov/files/company_tickers.json"
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    mapping = {}
    for entry in data.values():
        mapping[entry["ticker"]] = str(entry["cik_str"]).zfill(10)
    return mapping


def extract_metric_records(ticker, cik, facts_json):
    records = []
    us_gaap = facts_json.get("facts", {}).get("us-gaap", {})

    metric_map = {
        "Revenues": "revenue",
        "EarningsPerShareBasic": "eps_basic",
        "Assets": "assets",
        "OperatingIncomeLoss": "operating_income",
        "LongTermDebt": "long_term_debt",
    }

    for sec_metric, friendly_name in metric_map.items():
        metric_block = us_gaap.get(sec_metric, {})
        units = metric_block.get("units", {})

        for unit_name, values in units.items():
            for row in values:
                val = row.get("val")
                if val is None:
                    continue

                records.append(
                    {
                        "ticker": ticker,
                        "cik": cik,
                        "metric_name": friendly_name,
                        "sec_metric_name": sec_metric,
                        "unit": unit_name,
                        "value": val,
                        "fy": row.get("fy"),
                        "fp": row.get("fp"),
                        "filed": row.get("filed"),
                        "form": row.get("form"),
                        "frame": row.get("frame"),
                        "source": "sec_edgar",
                        "ingested_at": datetime.utcnow().isoformat(),
                    }
                )

    return records


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cik_map = fetch_ticker_cik_map()

    all_records = []

    for ticker in TICKERS:
        cik = cik_map.get(ticker)
        if not cik:
            print(f"Skipping {ticker}: no CIK found")
            continue

        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
            resp.raise_for_status()
            facts_json = resp.json()

            records = extract_metric_records(ticker, cik, facts_json)
            all_records.extend(records)

            print(f"{ticker}: extracted {len(records)} records")

        except Exception as exc:
            print(f"Error for {ticker}: {exc}")

    output_file = os.path.join(
        OUTPUT_DIR,
        f"edgar_facts_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
    )

    with open(output_file, "w") as f:
        json.dump(all_records, f, indent=2)

    print(f"Saved {len(all_records)} total records to {output_file}")


if __name__ == "__main__":
    main()