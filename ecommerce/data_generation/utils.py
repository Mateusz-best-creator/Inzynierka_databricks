"""Shared utility functions and pre-cached Faker pools for synthetic data generation.

Provides seed initialization, pre-cached string pools for fast
entity generation, date/SCD Type 2 timeline generators, weighted order date
and item quantity generators, and search lookups for historical pricing
and catalog status.
"""

import bisect
from datetime import date, datetime, timedelta
import random
from typing import Any

import config
from faker import Faker

# Seed initialization
random.seed(config.RANDOM_SEED)
Faker.seed(config.RANDOM_SEED)
fake = Faker("en_US")

# -----------------------------------------------------------------------------
# PRE-CACHED STRING POOLS AND CONSTANTS
# -----------------------------------------------------------------------------
POOL_SIZE = 5000
SMALL_POOL_SIZE = 1000

DEFAULT_STATE = "California"
DEFAULT_FALLBACK_PRICE = 50.0
INELIGIBLE_USER_STATUSES = ("INACTIVE", "SUSPENDED", "BANNED", "DELETED")

print("[INIT] Building pre-cached string pools...")

POOL_FIRST_MALE = [fake.first_name_male() for _ in range(POOL_SIZE // 2)]
POOL_FIRST_FEMALE = [fake.first_name_female() for _ in range(POOL_SIZE // 2)]
POOL_FIRST_NEUTRAL = [fake.first_name() for _ in range(POOL_SIZE // 5)]
POOL_LAST_NAMES = [fake.last_name() for _ in range(POOL_SIZE)]

POOL_CITIES = [fake.city() for _ in range(SMALL_POOL_SIZE)]
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
POOL_ZIPCODES = [fake.zipcode() for _ in range(SMALL_POOL_SIZE)]

POOL_COMPANIES = [fake.company() for _ in range(SMALL_POOL_SIZE)]
POOL_DESCRIPTIONS = [fake.paragraph(nb_sentences=2) for _ in range(SMALL_POOL_SIZE)]
POOL_PHONES = [fake.phone_number() for _ in range(POOL_SIZE)]


# -----------------------------------------------------------------------------
# SHARED UTILITY FUNCTIONS
# -----------------------------------------------------------------------------
def format_csv_value(value: Any) -> Any:
    """Formats datetime and date objects into string representations for CSV export.

    Args:
        value: Value to format.

    Returns:
        Formatted timestamp string for datetime/date instances, or the original
        value unchanged for all other types.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return value


def random_date(start: datetime, end: datetime) -> datetime:
    """Generates a random datetime between two boundary timestamps.

    Args:
        start: Lower bound datetime.
        end: Upper bound datetime.

    Returns:
        Randomly sampled datetime within [start, end).
    """
    delta = end - start
    random_days = random.randrange(delta.days)
    random_seconds = random.randrange(24 * 60 * 60)
    return start + timedelta(days=random_days, seconds=random_seconds)


def generate_scd2_timeline(
    start_date: datetime, number_of_versions: int
) -> list[datetime]:
    """Generates a chronologically ordered list of SCD Type 2 datetimes.

    Each subsequent version advances by 10 to 300 days and stops if a generated
    timestamp exceeds current time.

    Args:
        start_date: Initial start timestamp for version 1.
        number_of_versions: Maximum number of historical versions to generate.

    Returns:
        Ordered list of datetimes.
    """
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


def build_weighted_date_sampler(
    start_date: datetime, end_date: datetime
) -> tuple[list[datetime], list[float]]:
    """Builds daily dates and seasonal traffic weights across a date range.

    Applies higher weights during peak retail periods such as late November
    (Black Friday/Cyber Monday), December pre-holiday shopping, and May-June.

    Args:
        start_date: Start date in the sampling window.
        end_date: End date of the sampling window.

    Returns:
        Tuple containing the list of daily datetimes and their corresponding
        relative weights.
    """
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


def generate_realistic_order_date(
    days_list: list[datetime], day_weights: list[float]
) -> datetime:
    """Samples a realistic order timestamp using seasonal daily and hourly weights.

    Args:
        days_list: Candidate daily datetimes.
        day_weights: Relative weights corresponding to each day in days_list.

    Returns:
        Sampled datetime combining a weighted day, diurnal hour, and random
        minute/second.
    """
    base_day = random.choices(days_list, weights=day_weights, k=1)[0]
    hour = random.choices(range(24), weights=config.HOURLY_WEIGHTS, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return base_day.replace(hour=hour, minute=minute, second=second)


def get_realistic_quantity(unit_price: float) -> int:
    """Samples an item purchase quantity based on unit price.

    Args:
        unit_price: Product unit price used to determine quantity probabilities.

    Returns:
        Sampled integer quantity between 1 and 5.
    """
    if unit_price >= 300.0:
        return random.choices([1, 2], weights=[95, 5])[0]
    elif unit_price >= 100.0:
        return random.choices([1, 2, 3], weights=[80, 17, 3])[0]
    elif unit_price >= 30.0:
        return random.choices([1, 2, 3], weights=[60, 30, 10])[0]
    else:
        return random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]


def build_user_state_lookup(
    user_location_table: list[dict], location_table: list[dict]
) -> dict[str, list[dict]]:
    """Maps each user_id to their address history with state names.

    Args:
        user_location_table: User-to-location history records.
        location_table: Location records containing location_id and state.

    Returns:
        Dictionary mapping user_id to a list of state validity periods.
    """
    loc_state_map = {loc["location_id"]: loc["state"] for loc in location_table}
    user_locs: dict[str, list[dict]] = {}
    for ul in user_location_table:
        user_locs.setdefault(ul["user_id"], []).append(
            {
                "valid_from": ul["valid_from"],
                "valid_to": ul["valid_to"],
                "state": loc_state_map.get(str(ul["location_id"]), DEFAULT_STATE),
            }
        )
    return user_locs


def get_user_state_at_date(
    user_id: str, order_date: datetime, user_locs_map: dict[str, list[dict]]
) -> str:
    """Finds a user's US state at a given order date.

    Args:
        user_id: Target user ID.
        order_date: Date and time of the order.
        user_locs_map: Lookup dictionary returned by build_user_state_lookup.

    Returns:
        Active US state name at order_date, or DEFAULT_STATE if none is found.
    """
    if user_id not in user_locs_map:
        return DEFAULT_STATE
    for period in user_locs_map[user_id]:
        if period["valid_from"] <= order_date and (
            period["valid_to"] is None or order_date < period["valid_to"]
        ):
            return period["state"]
    return user_locs_map[user_id][-1]["state"]


def build_user_status_lookup(
    user_status_history_table: list[dict],
) -> dict[str, list[dict]]:
    """Groups user account status history records by user_id.

    Args:
        user_status_history_table: List of user status history records.

    Returns:
        Dictionary mapping user_id to its status history rows.
    """
    status_map: dict[str, list[dict]] = {}
    for row in user_status_history_table:
        status_map.setdefault(row["user_id"], []).append(row)
    return status_map


def is_user_eligible_to_order_at_date(
    user_id: str, order_date: datetime, user_status_map: dict[str, list[dict]]
) -> bool:
    """Checks whether a user account was eligible to place an order at order_date.

    Users with INACTIVE, SUSPENDED, BANNED, or DELETED status during the target
    period are excluded from placing orders.

    Args:
        user_id: Target user ID.
        order_date: Date and time of the order.
        user_status_map: Lookup dictionary returned by user_status_lookup.

    Returns:
        True if the user was eligible to order at order_date, False otherwise.
    """
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
    return last_status not in INELIGIBLE_USER_STATUSES


def calculate_freight_value(
    state: str, shipping_method: str, total_weight_g: float
) -> float:
    """Calculates shipping cost from destination state, shipping method, and weight.

    Args:
        state: Destination US state name.
        shipping_method: Shipping tier (for example STANDARD or EXPRESS).
        total_weight_g: Total shipment weight in grams.

    Returns:
        Shipping freight cost, rounded to 2 decimal places.
    """
    base_cost = config.STATE_BASE_FREIGHT.get(
        state, config.STATE_BASE_FREIGHT["DEFAULT"]
    )
    weight_cost = (total_weight_g / 1000.0) * config.FREIGHT_PER_KG
    multiplier = config.SHIPPING_MULTIPLIERS.get(shipping_method, 1.0)
    return round((base_cost + weight_cost) * multiplier, 2)


def is_product_active_at_date(
    product_id: str, order_date: datetime, catalog_bisect: dict[str, tuple]
) -> bool:
    """Checks if a product was active in the catalog at a given order date.

    Args:
        product_id: Target product ID.
        order_date: Date and time of the order.
        catalog_bisect: Catalog lookup dictionary returned by build_product_lookups.

    Returns:
        True if the product was active at order_date, False otherwise.
    """
    if product_id not in catalog_bisect:
        return True
    starts, periods = catalog_bisect[product_id]
    idx = bisect.bisect_right(starts, order_date) - 1
    if idx >= 0:
        valid_from, valid_to, is_active = periods[idx]
        if valid_to is None or order_date < valid_to:
            return is_active == 1
    return periods[-1][2] == 1


def build_product_lookups(
    product_catalog: list[dict], product_pricing: list[dict]
) -> tuple[dict[str, tuple], dict[str, tuple]]:
    """Builds sorted lookup structures for fast product status and price checks.

    Args:
        product_catalog: Product catalog history records.
        product_pricing: Product pricing history records.

    Returns:
        Tuple of (catalog_bisect, pricing_bisect) dictionaries keyed by product_id.
    """
    catalog_lookup: dict[str, list[tuple]] = {}
    for row in product_catalog:
        p_id = row["product_id"]
        if p_id not in catalog_lookup:
            catalog_lookup[p_id] = []
        catalog_lookup[p_id].append(
            (row["valid_from"], row["valid_to"], row["is_active"])
        )

    catalog_bisect: dict[str, tuple] = {}
    for p_id, periods in catalog_lookup.items():
        periods.sort(key=lambda x: x[0])
        catalog_bisect[p_id] = ([p[0] for p in periods], periods)

    pricing_lookup: dict[str, list[tuple]] = {}
    for row in product_pricing:
        p_id = row["product_id"]
        if p_id not in pricing_lookup:
            pricing_lookup[p_id] = []
        pricing_lookup[p_id].append((row["valid_from"], row["valid_to"], row["price"]))

    pricing_bisect: dict[str, tuple] = {}
    for p_id, periods in pricing_lookup.items():
        periods.sort(key=lambda x: x[0])
        pricing_bisect[p_id] = ([p[0] for p in periods], periods)

    return catalog_bisect, pricing_bisect


def get_historical_price(
    product_id: str, order_date: datetime, pricing_bisect: dict[str, tuple]
) -> float:
    """Gets a product's unit price at a given order date.

    Args:
        product_id: Target product ID.
        order_date: Date and time of the order.
        pricing_bisect: Pricing lookup dictionary returned by build_product_lookups.

    Returns:
        Product price at order_date, or DEFAULT_FALLBACK_PRICE if not found.
    """
    if product_id not in pricing_bisect:
        return DEFAULT_FALLBACK_PRICE
    starts, periods = pricing_bisect[product_id]
    idx = bisect.bisect_right(starts, order_date) - 1
    if idx >= 0:
        valid_from, valid_to, price = periods[idx]
        if valid_to is None or order_date < valid_to:
            return price
    return periods[-1][2]
