import boto3
import json
import os
import requests
from datetime import datetime, timezone


# Set this in Lambda environment variables
API_KEY = os.environ["EXCHANGERATE_API_KEY"]
STREAM_NAME = os.environ["FIREHOSE_STREAM_NAME"]
REGION = os.environ['AWS_REGION']

firehose = boto3.client("firehose", region_name=REGION)

BASE_CURRENCY = "USD"

# Currencies relevant to the RetailPulse order dataset
# (matches CURRENCIES list in shared_config.py)
TARGET_CURRENCIES = [
    "EUR", "GBP", "INR", "AED", "SGD", "JPY", "AUD",
    "CAD", "CHF", "CNY", "HKD", "KRW", "MYR", "THB",
    "SAR", "QAR", "BRL", "MXN", "ZAR", "NZD",
]


def fetch_rates() -> dict:
    """
    Calls the v6 /latest/{base} endpoint.
    Returns the full parsed JSON response.
    Raises on non-200 or API-level error.
    """
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{BASE_CURRENCY}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()
    if data.get("result") != "success":
        raise ValueError(f"API error: {data.get('error-type', 'unknown')}")

    return data


def build_records(data: dict, run_id: str) -> list[dict]:
    """
    Flattens the rates dict into one record per currency pair.
    Only includes TARGET_CURRENCIES — avoids sending 160+ irrelevant rows.
    """
    rates = data["conversion_rates"]          # e.g. {"EUR": 0.92, ...}
    rate_date = data["time_last_update_utc"]       # e.g. "Thu, 17 Apr 2025 00:00:01 +0000"
    next_update = data["time_next_update_utc"]
    last_unix = data["time_last_update_unix"]
    ingested_at = datetime.now(timezone.utc).isoformat()
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    records = []
    for target in TARGET_CURRENCIES:
        rate = rates.get(target)
        if rate is None:
            print(f"[WARN] {target} not found in API response — skipping")
            continue

        records.append({
            "base_currency": BASE_CURRENCY,
            "target_currency": target,
            "exchange_rate": rate,
            "rate_date_utc": rate_date,
            "rate_unix": last_unix,
            "next_update_utc": next_update,
            "pair": f"{BASE_CURRENCY}/{target}",   # e.g. USD/EUR
            "dataset": "bronze_fx_rates",
            "source": "exchangerate_api_v6",
            "run_id": run_id,
            "ingested_at": ingested_at,
            "ingestion_date": ingestion_date
        })

    return records


def send_to_firehose(records: list[dict]) -> int:
    firehose_records = [
        {"Data": (json.dumps(r) + "\n").encode("utf-8")}
        for r in records
    ]

    sent = 0
    for i in range(0, len(firehose_records), 500):
        batch = firehose_records[i : i + 500]
        response = firehose.put_record_batch(
            DeliveryStreamName=STREAM_NAME,
            Records=batch,
        )
        failed = response.get("FailedPutCount", 0)
        if failed:
            print(f"[WARN] {failed} records failed in batch at index {i}")
        sent += len(batch) - failed

    return sent


def lambda_handler(event, context):
    run_id = context.aws_request_id
    print(f"[fx_rates] run_id={run_id}")

    data = fetch_rates()
    records = build_records(data, run_id)
    print(f"[fx_rates] built {len(records)} currency pair records")

    sent = send_to_firehose(records)
    print(f"[fx_rates] sent {sent} records → dataset=bronze_fx_rates")

    return {
        "statusCode": 200,
        "base_currency": BASE_CURRENCY,
        "pairs_sent": sent,
        "rate_date": data["time_last_update_utc"],
        "run_id": run_id,
    }