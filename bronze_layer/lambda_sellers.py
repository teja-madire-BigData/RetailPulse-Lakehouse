import boto3
import os
import json
import random
from datetime import datetime, timezone, timedelta

from shared_config import (
    CATALOGUE_SEED, CATEGORY_NAMES,
    make_catalogue_faker, make_event_faker, wchoice,
)

# Set this in Lambda environment variables
REGION = os.environ['AWS_REGION']
STREAM_NAME = os.environ["FIREHOSE_STREAM_NAME"]

firehose = boto3.client("firehose", region_name=REGION)

NUM_SELLERS = 100

ACCOUNT_STATUSES = ["active", "inactive", "suspended", "under_review"]
STATUS_WEIGHTS = [0.82,    0.08,       0.05,         0.05]

COUNTRIES = [
    "United States", "United Kingdom", "India", "Germany",
    "Canada", "Australia", "Singapore", "UAE", "China", "Japan"
]
COUNTRY_WEIGHTS = [0.25, 0.12, 0.20, 0.08, 0.08, 0.07, 0.06, 0.06, 0.05, 0.03]

# Commission rate by category
COMMISSION_RATES = {
    "Electronics": 0.08,
    "Clothing": 0.12,
    "Groceries": 0.05,
    "Home": 0.10,
    "Beauty": 0.13,
    "Sports": 0.10,
    "Books": 0.07,
    "Toys": 0.11,
}


def build_seller_catalogue(fake) -> list[dict]:
    sellers = []
    rng = random.Random(CATALOGUE_SEED)
    now = datetime.now(timezone.utc)

    for i in range(1, NUM_SELLERS + 1):
        category = rng.choice(CATEGORY_NAMES)
        days_ago = rng.randint(90, 5 * 365)
        joined_at = now - timedelta(days=days_ago)
        country = rng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]

        sellers.append({
            "seller_id": i,
            "seller_name": fake.company(),
            "email": fake.company_email(),
            "country": country,
            "city": fake.city(),
            "category_specialisation": category,
            "commission_rate": COMMISSION_RATES[category],
            "joined_at": joined_at.isoformat(),
        })

    return sellers


def simulate_seller_state(seller: dict) -> dict:
    now = datetime.now(timezone.utc)

    # Rating drifts between 3.0 and 5.0
    seller_rating = round(random.uniform(3.0, 5.0), 1)
    total_reviews = random.randint(10, 5000)
    is_verified = random.random() > 0.15      # 85% verified
    account_status = wchoice(ACCOUNT_STATUSES, STATUS_WEIGHTS)
    total_products = random.randint(5, 500)
    fulfillment_score = round(random.uniform(60.0, 100.0), 1)  # % on-time delivery

    return {
        **seller,
        "seller_rating": seller_rating,
        "total_reviews": total_reviews,
        "is_verified": is_verified,
        "account_status": account_status,
        "total_products_listed": total_products,
        "fulfillment_score": fulfillment_score,
    }


def send_to_firehose(records: list[dict], run_id: str) -> int:
    ingested_at = datetime.now(timezone.utc).isoformat()
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    firehose_records = []
    for r in records:
        payload = {
            **r,
            "dataset": "bronze_sellers",
            "source": "faker_synthetic",
            "run_id": run_id,
            "ingested_at": ingested_at,
            "ingestion_date": ingestion_date,
        }
        firehose_records.append({
            "Data": (json.dumps(payload) + "\n").encode("utf-8")
        })

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
    print(f"[sellers] run_id={run_id}")

    cat_fake = make_catalogue_faker()
    catalogue = build_seller_catalogue(cat_fake)
    print(f"[sellers] catalogue built: {len(catalogue)} sellers")

    _evt_fake = make_event_faker()
    snapshots = [simulate_seller_state(s) for s in catalogue]

    sent = send_to_firehose(snapshots, run_id)
    print(f"[sellers] sent {sent} records → dataset=bronze_sellers")

    return {
        "statusCode": 200,
        "sellers_sent": sent,
        "run_id": run_id,
    }