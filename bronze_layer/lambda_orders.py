import boto3
import os
import json
import random
import uuid
from datetime import datetime, timezone, timedelta

from shared_config import (
    NUM_PRODUCTS, NUM_USERS,
    CATEGORIES, CATEGORY_NAMES, CATEGORY_PRICE_BANDS,
    ORDER_STATUSES, STATUS_WEIGHTS,
    PAYMENT_METHODS, PAYMENT_WEIGHTS,
    ORDER_CHANNELS, CHANNEL_WEIGHTS,
    CURRENCIES, CURRENCY_WEIGHTS,
    make_event_faker, wchoice,
)

# Set this in Lambda environment variables
REGION = os.environ['REGION']
STREAM_NAME = os.environ["FIREHOSE_STREAM_NAME"]

firehose = boto3.client("firehose", region_name=REGION)


def build_seller_catalogue():
    """Build mapping of category → seller_ids using same seed=42 as lambda_sellers."""
    from shared_config import CATALOGUE_SEED
    rng = random.Random(CATALOGUE_SEED)
    mapping = {cat: [] for cat in CATEGORY_NAMES}
    for seller_id in range(1, 101):
        cat = rng.choice(CATEGORY_NAMES)
        mapping[cat].append(seller_id)
    # Ensure every category has at least one seller
    for cat, sellers in mapping.items():
        if not sellers:
            mapping[cat] = [rng.randint(1, 100)]
    return mapping
 
SELLER_CATALOGUE = build_seller_catalogue()
 

# Orders per run
MIN_ORDERS = 150
MAX_ORDERS = 200

# Delivery SLA by status (days from order to delivery)
DELIVERY_DAYS: dict[str, tuple[int, int] | None] = {
    "pending":          None,
    "processing":       None,
    "shipped":          (3, 14),
    "out_for_delivery": (1, 3),
    "delivered":        (2, 14),
    "cancelled":        None,
    "returned":         (2, 7),
    "refunded":         (5, 14),
}

# Shipping cost bands by channel
SHIPPING_COST_RANGE: dict[str, tuple[float, float]] = {
    "mobile_app":   (0.0,  9.99),
    "web":          (0.0,  9.99),
    "in_store":     (0.0,  0.0),
    "third_party":  (2.99, 14.99),
}

# Line item builder
def generate_line_items(num_items: int) -> tuple[list[dict], float, float]:
    """
    Builds realistic cart line items referencing stable product catalogue IDs.

    Returns
    -------
    items       : list of line-item dicts
    gross_total : sum of (price × qty) before discounts
    net_total   : sum of (final_price × qty) after discounts
    """
    # Bias toward one category per order (realistic basket behaviour)
    primary_category = random.choice(CATEGORY_NAMES)
    items = []
    gross_total = 0.0
    net_total = 0.0

    for _ in range(num_items):
        # ~65% of items from the primary category
        if random.random() < 0.65:
            category = primary_category
        else:
            category = random.choice(CATEGORY_NAMES)

        subcategory = random.choice(CATEGORIES[category])
        product_id = random.randint(1, NUM_PRODUCTS)   # stable catalogue ref
        min_p, max_p = CATEGORY_PRICE_BANDS[category]
        unit_price = round(random.uniform(min_p, max_p), 2)
        quantity = random.randint(1, 4)
        discount_pct = round(random.uniform(0, 30), 1) if random.random() < 0.25 else 0.0
        final_unit_price = round(unit_price * (1 - discount_pct / 100), 2)
        line_gross = round(unit_price * quantity, 2)
        line_net = round(final_unit_price * quantity, 2)

        # Assign seller from the category's seller pool
        seller_pool = SELLER_CATALOGUE.get(category, [1])
        seller_id   = random.choice(seller_pool)

        items.append({
            "product_id": product_id,
            "seller_id": seller_id,
            "category": category,
            "subcategory": subcategory,
            "unit_price": unit_price,
            "quantity": quantity,
            "discount_pct": discount_pct,
            "final_unit_price": final_unit_price,
            "line_gross_total": line_gross,
            "line_net_total": line_net,
        })

        gross_total += line_gross
        net_total += line_net

    # top_seller_id — seller with highest line_net_total in this order
    top_seller_id = max(items, key=lambda x: x["line_net_total"])["seller_id"]

    return items, round(gross_total, 2), round(net_total, 2), top_seller_id


# Order builder
def generate_order(fake) -> dict:
    """
    Builds one complete order record with header + line-item summary.
    order_id is a UUID — guaranteed unique across all runs.
    user_id references the stable 1,000-user catalogue.
    """
    now = datetime.now(timezone.utc)
    order_id = str(uuid.uuid4())
    user_id = random.randint(1, NUM_USERS)

    # Order timestamp — within the last 30 days
    ordered_at = now - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    status = wchoice(ORDER_STATUSES, STATUS_WEIGHTS)
    channel = wchoice(ORDER_CHANNELS, CHANNEL_WEIGHTS)
    currency = wchoice(CURRENCIES, CURRENCY_WEIGHTS)
    payment = wchoice(PAYMENT_METHODS, PAYMENT_WEIGHTS)

    # Delivery date — only for statuses that have progressed past shipping
    delivery_range = DELIVERY_DAYS.get(status)
    if delivery_range:
        days_to_deliver = random.randint(*delivery_range)
        estimated_delivery = ordered_at + timedelta(days=days_to_deliver)
    else:
        estimated_delivery = None

    # Line items
    num_items = random.randint(1, 6)
    items, gross, net, top_seller_id = generate_line_items(num_items)
    shipping_lo, shipping_hi = SHIPPING_COST_RANGE[channel]
    shipping_cost = round(random.uniform(shipping_lo, shipping_hi), 2)
    tax_rate = round(random.uniform(0.05, 0.18), 3)
    tax_amount = round(net * tax_rate, 2)
    order_total = round(net + shipping_cost + tax_amount, 2)

    # Derived summary columns (sit alongside products_json in Bronze)
    top_category = max(
        {item["category"] for item in items},
        key=lambda c: sum(i["quantity"] for i in items if i["category"] == c),
    )

    return {
        # identifiers
        "order_id": order_id,
        "user_id": user_id,

        # order header
        "order_status": status,
        "channel": channel,
        "payment_method": payment,
        "currency": currency,
        "ordered_at": ordered_at.isoformat(),
        "estimated_delivery_at": estimated_delivery.isoformat() if estimated_delivery else None,

        # financials
        "gross_total": gross,
        "discount_total": round(gross - net, 2),
        "net_total": net,
        "shipping_cost": shipping_cost,
        "tax_rate": tax_rate,
        "tax_amount": tax_amount,
        "order_total": order_total,

        # line-item summary (Bronze summary columns)
        "item_count": num_items,
        "total_quantity": sum(i["quantity"] for i in items),
        "top_category": top_category,

        # raw line items (preserved for Silver parsing)
        "top_seller_id": top_seller_id,
        "products_json": json.dumps(items),

        # flags
        "is_gift": random.random() < 0.07,
        "promo_code_used": random.random() < 0.18,
    }


# Firehose sender

def send_to_firehose(records: list[dict], run_id: str) -> int:
    ingested_at = datetime.now(timezone.utc).isoformat()
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    firehose_records = []
    for r in records:
        payload = {
            **r,
            "dataset": "bronze_orders",
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


# Handler

def lambda_handler(event, context):
    run_id = context.aws_request_id
    print(f"[orders] run_id={run_id}")

    # Event seed — different every run → new order IDs and timestamps
    fake = make_event_faker()

    num_orders = random.randint(MIN_ORDERS, MAX_ORDERS)
    print(f"[orders] generating {num_orders} orders this run")

    orders = [generate_order(fake) for _ in range(num_orders)]

    sent = send_to_firehose(orders, run_id)
    print(f"[orders] sent {sent} records → dataset=bronze_orders")

    return {
        "statusCode": 200,
        "orders_sent": sent,
        "run_id": run_id,
    }