import os
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIRECTORY = os.path.join(SCRIPT_DIR, "generated_combined_data")

# -----------------------------------------------------------------------------
# GLOBAL CONFIGURATION & TOGGLES
# -----------------------------------------------------------------------------
RANDOM_SEED = None  # Set to an integer for reproducible builds
INJECT_ERRORS = False  # Set to True for pipeline sanitization tests

NUMBER_OF_USERS = 25000
NUMBER_OF_LOCATIONS = 5000
NUMBER_OF_PRODUCTS = 5000
NUMBER_OF_CATEGORIES = 40
NUMBER_OF_ORDERS = 100000
NUMBER_OF_GENERAL_SESSIONS = 100000

# Error Rates (Used when INJECT_ERRORS = True)
NULL_RATE = 0.02
WRONG_PRICE_RATE = 0.03
WRONG_DATE_RATE = 0.02

INVALID_EMAILS = 0.03
INVALID_PHONE_NUMBERS = 0.03
NOT_SPECIFIED_GENDER_RATE = 1000

PRODUCT_MISSING_CAT_RATE = 0.02
PRODUCT_MISSING_NAME_RATE = 0.02
INVENTORY_NEGATIVE_STOCK_RATE = 0.03
INVENTORY_NEGATIVE_DIMENSION_RATE = 0.02
PRICING_NEGATIVE_PRICE_RATE = 0.03
PRICING_SCD2_GAP_RATE = 0.02
CATEGORY_ORPHAN_RATE = 0.05

PAYMENT_NEGATIVE_VALUE_RATE = 0.02
PAYMENT_INVALID_TYPE_RATE = 0.02
PAYMENT_INVALID_INSTALLMENTS_RATE = 0.02
PAYMENT_DUPLICATE_SEQ_RATE = 0.02
PAYMENT_STATUS_ANOMALY_RATE = 0.02

DISCONTINUATION_RATE = 0.25

# -----------------------------------------------------------------------------
# TIMELINE ANCHORS
# -----------------------------------------------------------------------------
NOW = datetime.now()
FOUNDATION_START = NOW - timedelta(days=365 * 4)
FOUNDATION_END = NOW - timedelta(days=365 * 3)

# -----------------------------------------------------------------------------
# BEHAVIORAL WEIGHTS & STRUCTURES
# -----------------------------------------------------------------------------
HOURLY_WEIGHTS = [
    3,
    2,
    1,
    1,
    1,
    2,
    4,
    6,
    7,
    8,
    8,
    7,
    7,
    7,
    7,
    8,
    9,
    10,
    12,
    12,
    11,
    9,
    6,
    4,
]

CATEGORY_STRUCTURE = {
    "Electronics": ["Laptops", "Smartphones", "Audio", "Cameras"],
    "Home & Garden": ["Furniture", "Kitchen", "Decor", "Lighting"],
    "Sports": ["Fitness", "Cycling", "Running", "Swimming"],
    "Clothing": ["Men", "Women", "Kids", "Accessories"],
}

CATEGORY_NOUNS = {
    "Laptops": ["Laptop", "Notebook", "Ultrabook"],
    "Smartphones": ["Smartphone", "Phone", "Phablet"],
    "Audio": ["Headphones", "Earbuds", "Speaker"],
    "Cameras": ["Camera", "DSLR", "Mirrorless"],
    "Furniture": ["Chair", "Table", "Sofa"],
    "Kitchen": ["Blender", "Pan", "Knife"],
    "Decor": ["Vase", "Painting", "Rug"],
    "Lighting": ["Bulb", "Desk Lamp", "Chandelier"],
    "Fitness": ["Yoga Mat", "Dumbbells", "Treadmill"],
    "Cycling": ["Bike", "Helmet", "Gloves"],
    "Running": ["Shoes", "Shorts", "Bottle"],
    "Swimming": ["Goggles", "Swimsuit", "Cap"],
    "Men": ["T-Shirt", "Jeans", "Jacket"],
    "Women": ["Dress", "Skirt", "Blouse"],
    "Kids": ["Toy", "T-Shirt", "Pants"],
    "Accessories": ["Watch", "Belt", "Wallet"],
}

STATE_BASE_FREIGHT = {
    "California": 10.0,
    "New York": 9.0,
    "Washington": 9.0,
    "Texas": 7.0,
    "Florida": 7.0,
    "Illinois": 6.0,
    "DEFAULT": 6.0,
}

FREIGHT_PER_KG = 0.8

SHIPPING_MULTIPLIERS = {
    "STANDARD": 1.0,
    "EXPRESS": 1.8,
    "SAME_DAY": 2.5,
    "PICKUP": 0.0,
}
