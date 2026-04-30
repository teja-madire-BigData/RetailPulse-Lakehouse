import boto3
import os
import json
import random
import re
from datetime import datetime, timezone, timedelta

from shared_config import (
    CATALOGUE_SEED, NUM_USERS,
    CATEGORY_NAMES,
    make_catalogue_faker, make_event_faker, wchoice,
)

# Set this in Lambda environment variables
REGION = os.environ['REGION']
STREAM_NAME = os.environ["FIREHOSE_STREAM_NAME"]

firehose = boto3.client("firehose", region_name=REGION)

# Simulation constants

ACCOUNT_STATUSES = ["active", "inactive", "suspended", "pending_verification"]
STATUS_WEIGHTS = [0.80, 0.10, 0.05, 0.05]

CUSTOMER_TIERS = ["bronze", "silver", "gold", "platinum"]
TIER_WEIGHTS = [0.50, 0.30, 0.15, 0.05]

PREFERRED_CATEGORIES = CATEGORY_NAMES   # reuses shared list

GENDERS = ["male", "female", "non_binary", "prefer_not_to_say"]
GENDER_WEIGHTS = [0.46, 0.46, 0.04, 0.04]

COUNTRIES = [
    "United States", "United Kingdom", "India", "Germany",
    "Canada", "Australia", "Singapore", "UAE",
]
COUNTRY_WEIGHTS = [0.30, 0.15, 0.20, 0.08, 0.08, 0.07, 0.06, 0.06]


def clean_phone(phone: str) -> str:
    """Strip extensions from Faker phone numbers (x634, ext123 etc.)"""
    if not phone:
        return phone
    phone = re.sub(r'x\d+$', '', phone, flags=re.IGNORECASE).strip()
    phone = re.sub(r'ext\.?\s*\d+$', '', phone, flags=re.IGNORECASE).strip()
    return phone

def build_user_catalogue(fake) -> list[dict]:
    """
    Generates 1,000 stable user profiles.
    PII-like fields (name, email, dob, address) are deterministic via seed=42
    so foreign-key joins from the orders table always resolve.
    """
    users = []
    rng = random.Random(CATALOGUE_SEED)
    now = datetime.now(timezone.utc)

    for i in range(1, NUM_USERS + 1):
        # Gender-consistent name generation
        gender = rng.choices(GENDERS, weights=GENDER_WEIGHTS, k=1)[0]
        if gender == "male":
            name = fake.name_male()
        elif gender == "female":
            name = fake.name_female()
        else:
            name = fake.name()

        # Stable account creation date (1–5 years ago)
        days_ago = rng.randint(30, 5 * 365)
        created_at = now - timedelta(days=days_ago)
        dob_year = rng.randint(1960, 2003)

        country = rng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]

        users.append({
            "user_id": i,
            "name": name,
            "email": fake.email(),
            "phone": clean_phone(fake.phone_number()),
            "gender": gender,
            "date_of_birth": f"{dob_year}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
            "country": country,
            "city": fake.city(),
            "address_line": fake.street_address(),
            "postal_code": fake.postcode(),
            "created_at": created_at.isoformat(),
            "preferred_category": rng.choice(PREFERRED_CATEGORIES),
            "marketing_opt_in": rng.random() > 0.35,
        })

    return users


# Per-run simulation (event faker — changes each invocation)

def simulate_user_state(user: dict) -> dict:
    """
    Applies behavioural state fields that change between runs.
    These are the fields that will trigger SCD Type 2 rows in Silver.
    """
    now = datetime.now(timezone.utc)

    # last_login: somewhere between now and 90 days ago
    last_login_days = random.randint(0, 90)
    last_login = now - timedelta(
        days=last_login_days,
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

    # session_count grows over time — base on user age + some noise
    created_at = datetime.fromisoformat(user["created_at"])
    account_days = max(1, (now - created_at).days)
    session_count = int(account_days * random.uniform(0.3, 1.8))

    # Tier can upgrade/downgrade slightly
    tier = wchoice(CUSTOMER_TIERS, [0.50, 0.30, 0.15, 0.05])

    # Account status — mostly active
    status = wchoice(ACCOUNT_STATUSES, STATUS_WEIGHTS)

    # Lifetime spend (correlates loosely with tier)
    tier_multiplier = {"bronze": 1, "silver": 3, "gold": 8, "platinum": 20}[tier]
    lifetime_spend = round(random.uniform(10, 500) * tier_multiplier, 2)

    # Cart and wishlist sizes
    cart_item_count = random.randint(0, 12)
    wishlist_item_count = random.randint(0, 30)

    return {
        **user,
        # ── behavioural / mutable state (SCD Type 2 triggers on these)
        "account_status": status,
        "customer_tier": tier,
        "last_login_at": last_login.isoformat(),
        "session_count": session_count,
        "lifetime_spend_usd": lifetime_spend,
        "cart_item_count": cart_item_count,
        "wishlist_item_count": wishlist_item_count,
        "is_verified": random.random() > 0.10,   # 90% verified
    }


# Firehose sender

def send_to_firehose(records: list[dict], run_id: str) -> int:
    ingested_at = datetime.now(timezone.utc).isoformat()
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    firehose_records = []
    for r in records:
        payload = {
            **r,
            "dataset": "bronze_users",
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
    print(f"[users] run_id={run_id}")

    cat_fake = make_catalogue_faker()
    catalogue = build_user_catalogue(cat_fake)
    print(f"[users] catalogue built: {len(catalogue)} users")

    _evt_fake = make_event_faker()
    snapshots = [simulate_user_state(u) for u in catalogue]

    # send to Firehose
    sent = send_to_firehose(snapshots, run_id)
    print(f"[users] sent {sent} records → dataset=bronze_users")

    return {
        "statusCode": 200,
        "users_sent": sent,
        "run_id": run_id,
    }