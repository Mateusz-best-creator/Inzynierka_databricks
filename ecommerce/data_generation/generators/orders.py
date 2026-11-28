"""Generates synthetic order headers, line items, pricing, and status histories.

Simulates Pareto-distributed customer order frequency, point-in-time active
product filtering, price-weighted basket selection, shipping freight calculation,
and SCD Type 2 order fulfillment status timelines.
"""

from datetime import datetime, timedelta
import random

import config
import numpy as np
import utils

SHIPPING_METHODS = ["STANDARD", "EXPRESS", "SAME_DAY", "PICKUP"]
SHIPPING_METHOD_WEIGHTS = [0.55, 0.25, 0.10, 0.10]
ITEM_COUNT_OPTIONS = [1, 2, 3, 4, 5]
ITEM_COUNT_WEIGHTS = [0.45, 0.30, 0.15, 0.07, 0.03]
DISCOUNT_OPTIONS = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25]
DISCOUNT_WEIGHTS = [0.50, 0.15, 0.15, 0.10, 0.05, 0.05]
ORDER_CHANNELS = ["WEB", "MOBILE_APP", "MARKETPLACE"]

CANDIDATE_SAMPLE_SIZE = 80
DEFAULT_ITEM_WEIGHT_G = 500
PARETO_ALPHA = 1.08
MAX_USER_WEIGHT = 800.0


def generate_order_status_history(
    created_at: datetime, paid_at: datetime, shipping_method: str
) -> list[dict]:
    """Generates an SCD Type 2 status timeline for an order.

    Calculates processing, shipping, and delivery timestamps based on the
    selected shipping tier and truncates any events beyond config.NOW.

    Args:
        created_at: Timestamp when the order was created.
        paid_at: Timestamp when payment completed.
        shipping_method: Selected shipping tier (SAME_DAY, EXPRESS, PICKUP,
            or STANDARD).

    Returns:
        List of status dictionaries with valid_from, valid_to, and
        is_current values.
    """
    if shipping_method == "SAME_DAY":
        processing_at = paid_at + timedelta(minutes=random.randint(15, 90))
        shipped_at = processing_at + timedelta(hours=random.randint(1, 4))
        delivered_at = shipped_at + timedelta(hours=random.randint(2, 8))
    elif shipping_method == "EXPRESS":
        processing_at = paid_at + timedelta(hours=random.randint(1, 8))
        shipped_at = processing_at + timedelta(hours=random.randint(6, 24))
        delivered_at = shipped_at + timedelta(
            days=random.randint(1, 2), hours=random.randint(1, 10)
        )
    elif shipping_method == "PICKUP":
        processing_at = paid_at + timedelta(hours=random.randint(1, 12))
        shipped_at = processing_at + timedelta(hours=random.randint(4, 24))
        delivered_at = shipped_at + timedelta(hours=random.randint(6, 48))
    elif shipping_method == "STANDARD":
        processing_at = paid_at + timedelta(hours=random.randint(4, 24))
        shipped_at = processing_at + timedelta(hours=random.randint(12, 72))
        delivered_at = shipped_at + timedelta(
            days=random.randint(3, 7), hours=random.randint(0, 12)
        )

    timeline = [
        ("CREATED", created_at),
        ("PENDING_PAYMENT", created_at + timedelta(seconds=random.randint(5, 60))),
        ("PAID", paid_at),
        ("PROCESSING", processing_at),
        ("SHIPPED", shipped_at),
        ("DELIVERED", delivered_at),
    ]
    timeline = [(status, ts) for status, ts in timeline if ts <= config.NOW]

    history: list[dict] = []
    for i, (status, valid_from) in enumerate(timeline):
        is_current = 1 if i == len(timeline) - 1 else 0
        history.append(
            {
                "status": status,
                "status_change_description": f"Status updated to {status}",
                "valid_from": valid_from,
                "valid_to": None if is_current else timeline[i + 1][1],
                "is_current": is_current,
            }
        )
    return history


def _sample_candidate_products(
    all_product_ids: list[str],
    created_at: datetime,
    num_items: int,
    catalog_bisect: dict[str, tuple],
    pricing_bisect: dict[str, tuple],
) -> list[str]:
    """Samples active product IDs weighted inversely by square-root price.

    Args:
        all_product_ids: Pool of all indexed product IDs.
        created_at: Order timestamp used to check catalog status and price.
        num_items: Number of line items to sample for the order.
        catalog_bisect: Pre-indexed catalog lookup from utils.build_product_lookups.
        pricing_bisect: Pre-indexed pricing lookup from utils.build_product_lookups.

    Returns:
        List of selected product IDs of length num_items.
    """
    raw_candidates = random.sample(
        all_product_ids, min(CANDIDATE_SAMPLE_SIZE, len(all_product_ids))
    )
    candidate_pids = [
        p_id
        for p_id in raw_candidates
        if utils.is_product_active_at_date(p_id, created_at, catalog_bisect)
    ]

    if not candidate_pids:
        candidate_pids = [
            p_id
            for p_id in all_product_ids
            if utils.is_product_active_at_date(p_id, created_at, catalog_bisect)
        ]

    cand_prices = [
        utils.get_historical_price(p_id, created_at, pricing_bisect)
        for p_id in candidate_pids
    ]
    cand_weights = [1.0 / (p**0.5) for p in cand_prices]
    return random.choices(candidate_pids, weights=cand_weights, k=num_items)


def _append_order_items(
    order_id: str,
    created_at: datetime,
    selected_products: list[str],
    order_discount_rate: float,
    global_item_counter: int,
    pricing_bisect: dict[str, tuple],
    weight_by_product: dict[str, int],
    order_items_table: list[dict],
    order_item_pricing_table: list[dict],
) -> tuple[int, float, int, list[dict]]:
    """Appends line item and item pricing records for a single order.

    Args:
        order_id: Parent order identifier.
        created_at: Order creation timestamp.
        selected_products: Product IDs chosen for each line item.
        order_discount_rate: Discount applied to each item.
        global_item_counter: Running sequential counter for order_item_id.
        pricing_bisect: Pre-indexed pricing lookup from utils.build_product_lookups.
        weight_by_product: Mapping of product_id to unit weight in grams.
        order_items_table: List collecting order line item rows.
        order_item_pricing_table: List collecting order item pricing rows.

    Returns:
        Tuple of (updated_item_counter, total_items_value, total_weight_g,
        order_items_meta with product_id and quantity).
    """
    items_value = 0.0
    total_weight_g = 0
    order_items_meta: list[dict] = []

    for seq, product_id in enumerate(selected_products, start=1):
        order_item_id = f"ORDER-ITEM-{str(global_item_counter).zfill(12)}"
        global_item_counter += 1

        unit_price = utils.get_historical_price(product_id, created_at, pricing_bisect)
        quantity = utils.get_realistic_quantity(unit_price)
        discount = round(unit_price * order_discount_rate, 2)

        items_value += round((unit_price - discount) * quantity, 2)
        total_weight_g += (
            weight_by_product.get(product_id, DEFAULT_ITEM_WEIGHT_G) * quantity
        )

        order_items_table.append(
            {
                "order_item_id": order_item_id,
                "order_id": order_id,
                "order_item_seq": seq,
                "product_id": product_id,
                "quantity": quantity,
            }
        )
        order_item_pricing_table.append(
            {
                "order_item_id": order_item_id,
                "unit_price": unit_price,
                "discount": discount,
            }
        )
        order_items_meta.append({"product_id": product_id, "quantity": quantity})

    return global_item_counter, items_value, total_weight_g, order_items_meta


def generate_orders_data(
    users_table: list[dict],
    product_catalog: list[dict],
    product_pricing: list[dict],
    product_inventory: list[dict],
    user_location_table: list[dict],
    location_table: list[dict],
    user_status_history_table: list[dict],
) -> tuple[
    list[dict],
    list[dict],
    list[dict],
    list[dict],
    list[dict],
    list[dict],
    list[dict],
]:
    """Generates all order domain tables and downstream payment/session metadata.

    Args:
        users_table: User dimension records.
        product_catalog: Product catalog SCD2 records.
        product_pricing: Product pricing SCD2 records.
        product_inventory: Product inventory and weight records.
        user_location_table: User-to-location SCD2 history records.
        location_table: Physical location dimension records.
        user_status_history_table: User account status SCD2 history records.

    Returns:
        Tuple of (orders_table, order_details_table, order_status_history_table,
        order_items_table, order_item_pricing_table, order_payment_info,
        order_purchase_meta).
    """
    orders_table: list[dict] = []
    order_details_table: list[dict] = []
    order_status_history_table: list[dict] = []
    order_items_table: list[dict] = []
    order_item_pricing_table: list[dict] = []
    order_payment_info: list[dict] = []
    order_purchase_meta: list[dict] = []

    user_locs_map = utils.build_user_state_lookup(user_location_table, location_table)
    user_status_map = utils.build_user_status_lookup(user_status_history_table)
    catalog_bisect, pricing_bisect = utils.build_product_lookups(
        product_catalog, product_pricing
    )

    weight_by_product = {
        row["product_id"]: row["weight_g"] for row in product_inventory
    }

    global_item_counter, global_status_history_counter = 1, 1
    user_ids = [u["user_id"] for u in users_table]
    all_product_ids = list(pricing_bisect.keys())

    batch_shipping_indices = np.random.choice(
        len(SHIPPING_METHODS), size=config.NUMBER_OF_ORDERS, p=SHIPPING_METHOD_WEIGHTS
    )
    batch_num_items = np.random.choice(
        ITEM_COUNT_OPTIONS, size=config.NUMBER_OF_ORDERS, p=ITEM_COUNT_WEIGHTS
    )
    batch_discount_rates = np.random.choice(
        DISCOUNT_OPTIONS, size=config.NUMBER_OF_ORDERS, p=DISCOUNT_WEIGHTS
    )

    user_weights = [
        min(random.paretovariate(alpha=PARETO_ALPHA), MAX_USER_WEIGHT) for _ in user_ids
    ]
    random.shuffle(user_weights)

    user_probs = np.array(user_weights, dtype=np.float64)
    user_probs /= user_probs.sum()
    pre_drawn_user_indices = np.random.choice(
        len(user_ids), size=config.NUMBER_OF_ORDERS * 2, p=user_probs
    )
    user_draw_pointer = 0

    days_list, day_weights = utils.build_weighted_date_sampler(
        config.FOUNDATION_END, config.NOW - timedelta(days=1)
    )

    for i in range(config.NUMBER_OF_ORDERS):
        order_id = f"ORDER-{str(i + 1).zfill(10)}"

        while True:
            if user_draw_pointer >= len(pre_drawn_user_indices):
                pre_drawn_user_indices = np.random.choice(
                    len(user_ids), size=config.NUMBER_OF_ORDERS, p=user_probs
                )
                user_draw_pointer = 0

            user_idx = pre_drawn_user_indices[user_draw_pointer]
            user_draw_pointer += 1
            user_id = user_ids[user_idx]

            created_at = utils.generate_realistic_order_date(days_list, day_weights)
            if utils.is_user_eligible_to_order_at_date(
                user_id, created_at, user_status_map
            ):
                break

        shipping_method = SHIPPING_METHODS[batch_shipping_indices[i]]
        payment_started_at = created_at + timedelta(minutes=random.randint(1, 20))
        paid_at = payment_started_at + timedelta(minutes=random.randint(1, 15))

        status_history = generate_order_status_history(
            created_at, paid_at, shipping_method
        )
        for status_record in status_history:
            order_status_history_table.append(
                {
                    "id": str(global_status_history_counter).zfill(10),
                    "order_id": order_id,
                    "status": status_record["status"],
                    "status_change_description": status_record[
                        "status_change_description"
                    ],
                    "valid_from": status_record["valid_from"],
                    "valid_to": status_record["valid_to"],
                    "is_current": status_record["is_current"],
                }
            )
            global_status_history_counter += 1

        selected_products = _sample_candidate_products(
            all_product_ids,
            created_at,
            int(batch_num_items[i]),
            catalog_bisect,
            pricing_bisect,
        )
        order_discount_rate = float(batch_discount_rates[i])

        (
            global_item_counter,
            items_value,
            total_weight_g,
            order_items_meta,
        ) = _append_order_items(
            order_id,
            created_at,
            selected_products,
            order_discount_rate,
            global_item_counter,
            pricing_bisect,
            weight_by_product,
            order_items_table,
            order_item_pricing_table,
        )

        state = utils.get_user_state_at_date(user_id, created_at, user_locs_map)
        freight_value = utils.calculate_freight_value(
            state, shipping_method, total_weight_g
        )

        order_details_table.append(
            {
                "order_id": order_id,
                "order_channel": random.choice(ORDER_CHANNELS),
                "shipping_method": shipping_method,
                "freight_value": freight_value,
            }
        )

        order_payment_info.append(
            {
                "order_id": order_id,
                "payment_started_at": payment_started_at,
                "paid_at": paid_at,
                "total_value": round(items_value + freight_value, 2),
            }
        )

        order_purchase_meta.append(
            {
                "order_id": order_id,
                "user_id": user_id,
                "created_at": created_at,
                "has_discount": order_discount_rate > 0,
                "items": order_items_meta,
            }
        )

    return (
        orders_table,
        order_details_table,
        order_status_history_table,
        order_items_table,
        order_item_pricing_table,
        order_payment_info,
        order_purchase_meta,
    )
