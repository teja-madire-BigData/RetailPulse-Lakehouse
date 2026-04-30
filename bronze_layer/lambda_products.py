import boto3
import os
import json
import random
from datetime import datetime, timezone

from shared_config import (
    CATALOGUE_SEED, NUM_PRODUCTS,
    CATEGORIES, CATEGORY_NAMES,
    CATEGORY_PRICE_BANDS, CATEGORY_PRICE_VOLATILITY, CATEGORY_STOCK_RANGE,
    make_catalogue_faker, make_event_faker, wchoice,
)

# Set this in Lambda environment variables
REGION = os.environ['REGION']
STREAM_NAME = os.environ["FIREHOSE_STREAM_NAME"]

firehose = boto3.client("firehose", region_name=REGION)

def build_seller_catalogue():
    rng = random.Random(CATALOGUE_SEED)
    mapping = {cat: [] for cat in CATEGORY_NAMES}
    for seller_id in range(1, 101):
        cat = rng.choice(CATEGORY_NAMES)
        mapping[cat].append(seller_id)
    for cat, sellers in mapping.items():
        if not sellers:
            mapping[cat] = [rng.randint(1, 100)]
    return mapping

SELLER_CATALOGUE = build_seller_catalogue()

# Catalogue builder (seed=42 — stable across all runs)
def build_product_catalogue(fake) -> list[dict]:
    """
    Generates the full 500-product catalogue.
    Called with the catalogue faker so IDs and names are deterministic.
    """
    products = []
    rng = random.Random(CATALOGUE_SEED)   # independent RNG for catalogue fields

    for i in range(1, NUM_PRODUCTS + 1):
        category = rng.choice(CATEGORY_NAMES)
        subcategory = rng.choice(CATEGORIES[category])
        min_p, max_p = CATEGORY_PRICE_BANDS[category]
        base_price  = round(rng.uniform(min_p, max_p), 2)
        seller_pool = SELLER_CATALOGUE.get(category, [1])
        seller_id   = rng.choice(seller_pool)  # use catalogue rng not random — stable assignment

        products.append({
            "product_id": i,
            "seller_id": seller_id,
            "sku": f"SKU-{i:05d}",
            "name": fake.catch_phrase(),
            "category": category,
            "subcategory": subcategory,
            "brand": fake.company(),
            "base_price": base_price,
            "weight_kg": round(rng.uniform(0.1, 20.0), 2),
            "is_active": rng.random() > 0.05,   # 95% active
        })

    return products


# Per-run simulation (event faker — changes each invocation)

def simulate_snapshot(product: dict) -> dict:
    """
    Applies run-time fluctuations on top of stable catalogue fields.
    Uses module-level random (seeded by timestamp in make_event_faker).
    """
    category = product["category"]
    base_price = product["base_price"]
    vol = CATEGORY_PRICE_VOLATILITY[category]
    s_min, s_max = CATEGORY_STOCK_RANGE[category]

    # Price fluctuation +/- vol%
    current_price = round(base_price * random.uniform(1 - vol, 1 + vol), 2)

    # Discount — applied ~22% of the time
    discount_active = random.random() < 0.22
    discount_pct = round(random.uniform(5, 40), 1) if discount_active else 0.0
    final_price = round(current_price * (1 - discount_pct / 100), 2) if discount_active else current_price

    # Stock
    stock_qty = random.randint(s_min, s_max)
    is_low_stock = stock_qty < (s_min + int((s_max - s_min) * 0.10))

    # Rating drifts slightly each run (+/- 0.1 around a stable base)
    rating_base = round(2.5 + random.random() * 2.5, 1)   # 2.5–5.0
    rating = round(max(1.0, min(5.0, rating_base + random.uniform(-0.1, 0.1))), 1)
    review_count = random.randint(0, 5000)

    # Orders in the last 15 min (simulates near-real-time demand signal)
    orders_15min = random.randint(0, 120)

    return {
        **product,
        # fluctuating fields
        "current_price": current_price,
        "discount_active": discount_active,
        "discount_pct": discount_pct,
        "final_price": final_price,
        "stock_quantity": stock_qty,
        "is_low_stock": is_low_stock,
        "rating": rating,
        "review_count": review_count,
        "orders_last_15min": orders_15min,
    }


# Firehose sender

def send_to_firehose(records: list[dict], run_id: str) -> int:
    ingested_at = datetime.now(timezone.utc).isoformat()
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    firehose_records = []
    for r in records:
        payload = {
            **r,
            "dataset": "bronze_products",
            "source": "faker_synthetic",
            "run_id": run_id,
            "ingested_at": ingested_at,
            "ingestion_date": ingestion_date
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


# Handler

def lambda_handler(event, context):
    run_id = context.aws_request_id
    print(f"[products] run_id={run_id}")

    # build stable catalogue (seed=42)
    cat_fake = make_catalogue_faker()
    catalogue = build_product_catalogue(cat_fake)
    print(f"[products] catalogue built: {len(catalogue)} products")

    # apply per-run simulation (event seed = timestamp)
    _evt_fake = make_event_faker()   # sets module random seed as side-effect
    snapshots = [simulate_snapshot(p) for p in catalogue]

    # send to Firehose
    sent = send_to_firehose(snapshots, run_id)
    print(f"[products] sent {sent} records → dataset=bronze_products")

    return {
        "statusCode": 200,
        "products_sent": sent,
        "run_id": run_id,
    }