import random
from faker import Faker

# Catalogue seed
# All three Lambdas use seed=42 when generating catalogue-level IDs
# (product_id, user_id) so Silver-layer joins always resolve across runs.
CATALOGUE_SEED = 42

# Counts
NUM_PRODUCTS = 500
NUM_USERS = 1000

# Categories & subcategories
CATEGORIES: dict[str, list[str]] = {
    "Electronics": ["Smartphones", "Laptops", "Tablets", "Cameras",
                    "Audio", "Wearables", "Gaming", "Accessories"],
    "Clothing": ["Men's Tops", "Women's Tops", "Dresses", "Jeans",
                 "Activewear", "Outerwear", "Footwear", "Innerwear"],
    "Groceries": ["Fruits & Veg", "Dairy & Eggs", "Snacks",
                  "Beverages", "Staples", "Bakery", "Frozen", "Condiments"],
    "Home": ["Furniture", "Bedding", "Kitchen", "Bath",
             "Decor", "Lighting", "Storage", "Cleaning"],
    "Beauty": ["Skincare", "Haircare", "Makeup", "Fragrances",
                "Men's Grooming", "Wellness", "Baby Care", "Tools"],
    "Sports": ["Fitness Equipment", "Outdoor", "Team Sports",
                "Swimming", "Cycling", "Yoga", "Footwear", "Nutrition"],
    "Books": ["Fiction", "Non-Fiction", "Science", "Technology",
              "Children's", "Comics", "Self-Help", "Biography"],
    "Toys": ["Action Figures", "Board Games", "Educational",
             "Dolls", "Remote Control", "Puzzles", "Outdoor Play", "Arts & Crafts"],
}

CATEGORY_NAMES = list(CATEGORIES.keys())

# Price bands per category (min, max) in USD
CATEGORY_PRICE_BANDS: dict[str, tuple[float, float]] = {
    "Electronics": (29.99, 2499.99),
    "Clothing": (9.99, 299.99),
    "Groceries": (0.99, 49.99),
    "Home": (14.99, 999.99),
    "Beauty": (4.99, 199.99),
    "Sports": (9.99, 599.99),
    "Books": (4.99, 79.99),
    "Toys": (4.99, 249.99),
}

# Price volatility per category (±fraction applied each run)
CATEGORY_PRICE_VOLATILITY: dict[str, float] = {
    "Electronics": 0.07,
    "Clothing": 0.12,
    "Groceries": 0.05,
    "Home": 0.10,
    "Beauty": 0.10,
    "Sports": 0.08,
    "Books": 0.03,
    "Toys": 0.09,
}

# Stock range per category (min, max units)
CATEGORY_STOCK_RANGE: dict[str, tuple[int, int]] = {
    "Electronics": (10, 300),
    "Clothing": (20, 500),
    "Groceries": (50, 2000),
    "Home": (5, 200),
    "Beauty": (30, 800),
    "Sports": (10, 400),
    "Books": (5, 1000),
    "Toys": (10, 600),
}

# Order simulation constants
ORDER_STATUSES = [
    "pending", "processing", "shipped",
    "out_for_delivery", "delivered",
    "cancelled", "returned", "refunded",
]
# Realistic distribution — most orders should be delivered or shipped
STATUS_WEIGHTS = [0.05, 0.10, 0.15, 0.08, 0.45, 0.08, 0.05, 0.04]

PAYMENT_METHODS = [
    "credit_card", "debit_card", "upi",
    "net_banking", "wallet", "cod",
]
PAYMENT_WEIGHTS = [0.28, 0.20, 0.27, 0.08, 0.10, 0.07]

ORDER_CHANNELS = ["mobile_app", "web", "in_store", "third_party"]
CHANNEL_WEIGHTS = [0.45, 0.35, 0.10, 0.10]

CURRENCIES = ["USD", "EUR", "GBP", "INR", "AED", "SGD"]
CURRENCY_WEIGHTS = [0.40, 0.20, 0.15, 0.15, 0.05, 0.05]


def make_catalogue_faker() -> Faker:
    """Returns a Faker instance seeded for stable catalogue generation."""
    f = Faker()
    Faker.seed(CATALOGUE_SEED)
    random.seed(CATALOGUE_SEED)
    return f


def make_event_faker() -> Faker:
    """
    Returns a Faker instance seeded from the current timestamp (seconds).
    Every Lambda run gets a different seed → genuinely new event records.
    """
    import time
    seed = int(time.time())
    f = Faker()
    Faker.seed(seed)
    random.seed(seed)
    return f


def wchoice(population: list, weights: list) -> str:
    """Convenience wrapper for random.choices returning a single item."""
    return random.choices(population, weights=weights, k=1)[0]