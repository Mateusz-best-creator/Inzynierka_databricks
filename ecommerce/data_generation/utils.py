import random
import bisect
from datetime import datetime, timedelta, date
from faker import Faker
import config

# Seed initialization
random.seed(config.RANDOM_SEED)
Faker.seed(config.RANDOM_SEED)
fake = Faker("en_US")

# -----------------------------------------------------------------------------
# PRE-CACHED STRING POOLS
# -----------------------------------------------------------------------------
POOL_SIZE = 5000

print("[INIT] Building pre-cached string pools...")

POOL_FIRST_MALE = [fake.first_name_male() for _ in range(POOL_SIZE // 2)]
POOL_FIRST_FEMALE = [fake.first_name_female() for _ in range(POOL_SIZE // 2)]
POOL_FIRST_NEUTRAL = [fake.first_name() for _ in range(POOL_SIZE // 5)]
POOL_LAST_NAMES = [fake.last_name() for _ in range(POOL_SIZE)]

POOL_CITIES = [fake.city() for _ in range(1000)]
POOL_STATES = [
    "California",
    "New York",
    "Texas",
    "Florida",
    "Illinois",
    "Washington",
    "Pennsylvania",
    "Ohio",
    "Georgia",
    "North Carolina",
]
POOL_ZIPCODES = [fake.zipcode() for _ in range(1000)]

POOL_COMPANIES = [fake.company() for _ in range(1000)]
POOL_DESCRIPTIONS = [fake.paragraph(nb_sentences=2) for _ in range(1000)]
POOL_PHONES = [fake.phone_number() for _ in range(POOL_SIZE)]


# -----------------------------------------------------------------------------
# SHARED UTILITY FUNCTIONS
# -----------------------------------------------------------------------------
def format_csv_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return value


def random_date(start, end):
    delta = end - start
    random_days = random.randrange(delta.days)
    random_seconds = random.randrange(24 * 60 * 60)
    return start + timedelta(days=random_days, seconds=random_seconds)


def generate_scd2_timeline(start_date, number_of_versions):
    if number_of_versions <= 0:
        return []

    dates = [start_date]
    for _ in range(number_of_versions - 1):
        next_date = dates[-1] + timedelta(
            days=random.randint(10, 300), hours=random.randint(1, 23)
        )
        if next_date > config.NOW:
            break
        dates.append(next_date)
    return dates


def build_weighted_date_sampler(start_date, end_date):
    total_days = (end_date - start_date).days
    days = [start_date + timedelta(days=i) for i in range(total_days)]
    weights = []

    for d in days:
        month = d.month
        day = d.day

        if month == 11:
            weight = 2.5 if 20 <= day <= 30 else 1.6
        elif month == 12:
            weight = 2.0 if day <= 23 else 1.2
        elif month in (5, 6):
            weight = 1.2
        else:
            weight = 1.0

        weights.append(weight)

    return days, weights


def generate_realistic_order_date(days_list, day_weights):
    base_day = random.choices(days_list, weights=day_weights, k=1)[0]
    hour = random.choices(range(24), weights=config.HOURLY_WEIGHTS, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return base_day.replace(hour=hour, minute=minute, second=second)


def get_realistic_quantity(unit_price: float) -> int:
    if unit_price >= 300.0:
        return random.choices([1, 2], weights=[95, 5])[0]
    elif unit_price >= 100.0:
        return random.choices([1, 2, 3], weights=[80, 17, 3])[0]
    elif unit_price >= 30.0:
        return random.choices([1, 2, 3], weights=[60, 30, 10])[0]
    else:
        return random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]


def build_user_state_lookup(user_location_table, location_table):
    loc_state_map = {loc["location_id"]: loc["state"] for loc in location_table}
    user_locs = {}
    for ul in user_location_table:
        user_locs.setdefault(ul["user_id"], []).append(
            {
                "valid_from": ul["valid_from"],
                "valid_to": ul["valid_to"],
                "state": loc_state_map.get(str(ul["location_id"]), "California"),
            }
        )
    return user_locs


def get_user_state_at_date(user_id, order_date, user_locs_map):
    if user_id not in user_locs_map:
        return "California"
    for period in user_locs_map[user_id]:
        if period["valid_from"] <= order_date and (
            period["valid_to"] is None or order_date < period["valid_to"]
        ):
            return period["state"]
    return user_locs_map[user_id][-1]["state"]


def build_user_status_lookup(user_status_history_table):
    status_map = {}
    for row in user_status_history_table:
        status_map.setdefault(row["user_id"], []).append(row)
    return status_map


def is_user_eligible_to_order_at_date(user_id, order_date, user_status_map):
    if user_id not in user_status_map:
        return True
    for row in user_status_map[user_id]:
        if row["valid_from"] <= order_date and (
            row["valid_to"] is None or order_date < row["valid_to"]
        ):
            if row["status"] in ("INACTIVE", "SUSPENDED", "BANNED", "DELETED"):
                return False
            return True

    last_status = user_status_map[user_id][-1]["status"]
    return last_status not in ("INACTIVE", "SUSPENDED", "BANNED", "DELETED")


def calculate_freight_value(
    state: str, shipping_method: str, total_weight_g: float
) -> float:
    base_cost = config.STATE_BASE_FREIGHT.get(
        state, config.STATE_BASE_FREIGHT["DEFAULT"]
    )
    weight_cost = (total_weight_g / 1000.0) * config.FREIGHT_PER_KG
    multiplier = config.SHIPPING_MULTIPLIERS.get(shipping_method, 1.0)
    return round((base_cost + weight_cost) * multiplier, 2)


# Add this helper to utils.py
def is_product_active_at_date_fast(
    product_id: str, order_date: datetime, catalog_bisect: dict
) -> bool:
    """Checks in O(log N) time if a product was active (is_active=1) at a given order date."""
    if product_id not in catalog_bisect:
        return True
    starts, periods = catalog_bisect[product_id]
    idx = bisect.bisect_right(starts, order_date) - 1
    if idx >= 0:
        valid_from, valid_to, is_active = periods[idx]
        if valid_to is None or order_date < valid_to:
            return is_active == 1
    return periods[-1][2] == 1


def build_optimized_product_lookups(product_catalog, product_pricing):
    """Pre-indexes catalog and pricing timelines into sorted tuples for O(log N) binary search."""
    catalog_lookup = {}
    for row in product_catalog:
        p_id = row["product_id"]
        if p_id not in catalog_lookup:
            catalog_lookup[p_id] = []
        catalog_lookup[p_id].append(
            (row["valid_from"], row["valid_to"], row["is_active"])
        )

    catalog_bisect = {}
    for p_id, periods in catalog_lookup.items():
        periods.sort(key=lambda x: x[0])
        catalog_bisect[p_id] = ([p[0] for p in periods], periods)

    pricing_bisect = {}
    for row in product_pricing:
        p_id = row["product_id"]
        if p_id not in pricing_bisect:
            pricing_bisect[p_id] = []
        pricing_bisect[p_id].append((row["valid_from"], row["valid_to"], row["price"]))

    for p_id, periods in pricing_bisect.items():
        periods.sort(key=lambda x: x[0])
        pricing_bisect[p_id] = ([p[0] for p in periods], periods)

    return catalog_bisect, pricing_bisect


def get_historical_price_fast(
    product_id: str, order_date: datetime, pricing_bisect: dict
) -> float:
    if product_id not in pricing_bisect:
        return 50.0
    starts, periods = pricing_bisect[product_id]
    idx = bisect.bisect_right(starts, order_date) - 1
    if idx >= 0:
        valid_from, valid_to, price = periods[idx]
        if valid_to is None or order_date < valid_to:
            return price
    return periods[-1][2]
