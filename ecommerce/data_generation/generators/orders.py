import random
import numpy as np
from datetime import timedelta
import config
import utils


def generate_order_status_history(created_at, paid_at, shipping_method):
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
    else:
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

    history = []
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


def generate_orders_data(
    users_table,
    product_catalog,
    product_pricing,
    product_inventory,
    user_location_table,
    location_table,
    user_status_history_table,
):
    orders_table, order_details_table, order_status_history_table = [], [], []
    order_items_table, order_item_pricing_table = [], []
    order_payment_info, order_purchase_meta = [], []

    user_locs_map = utils.build_user_state_lookup(user_location_table, location_table)
    user_status_map = utils.build_user_status_lookup(user_status_history_table)
    catalog_bisect, pricing_bisect = utils.build_optimized_product_lookups(
        product_catalog, product_pricing
    )

    weight_by_product = {
        row["product_id"]: row["weight_g"] for row in product_inventory
    }

    global_item_counter, global_status_history_counter = 1, 1
    user_ids = [u["user_id"] for u in users_table]
    all_product_ids = list(pricing_bisect.keys())
    shipping_methods = ["STANDARD", "EXPRESS", "SAME_DAY", "PICKUP"]

    batch_shipping_indices = np.random.choice(
        4, size=config.NUMBER_OF_ORDERS, p=[0.55, 0.25, 0.10, 0.10]
    )
    batch_num_items = np.random.choice(
        [1, 2, 3, 4, 5], size=config.NUMBER_OF_ORDERS, p=[0.45, 0.30, 0.15, 0.07, 0.03]
    )
    batch_discount_rates = np.random.choice(
        [0.0, 0.05, 0.10, 0.15, 0.20, 0.25],
        size=config.NUMBER_OF_ORDERS,
        p=[0.50, 0.15, 0.15, 0.10, 0.05, 0.05],
    )

    user_weights = [min(random.paretovariate(alpha=1.08), 800.0) for _ in user_ids]
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

        shipping_method = shipping_methods[batch_shipping_indices[i]]
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

        num_items = batch_num_items[i]

        # Subsample raw candidates and filter for active products at order creation date
        raw_candidates = random.sample(all_product_ids, min(80, len(all_product_ids)))
        candidate_pids = [
            p_id
            for p_id in raw_candidates
            if utils.is_product_active_at_date_fast(p_id, created_at, catalog_bisect)
        ]

        # Safety fallback
        if not candidate_pids:
            candidate_pids = [
                p_id
                for p_id in all_product_ids
                if utils.is_product_active_at_date_fast(
                    p_id, created_at, catalog_bisect
                )
            ]

        cand_prices = [
            utils.get_historical_price_fast(p_id, created_at, pricing_bisect)
            for p_id in candidate_pids
        ]
        cand_weights = [1.0 / (p**0.5) for p in cand_prices]

        selected_products = random.choices(
            candidate_pids, weights=cand_weights, k=num_items
        )

        items_value = 0.0
        total_weight_g = 0
        order_items_data_temp = []
        Order_discount_rate = batch_discount_rates[i]

        for seq, product_id in enumerate(selected_products, start=1):
            order_item_id = f"ORDER-ITEM-{str(global_item_counter).zfill(12)}"
            global_item_counter += 1

            unit_price = utils.get_historical_price_fast(
                product_id, created_at, pricing_bisect
            )
            quantity = utils.get_realistic_quantity(unit_price)
            discount = round(unit_price * Order_discount_rate, 2)

            items_value += round((unit_price - discount) * quantity, 2)
            total_weight_g += weight_by_product.get(product_id, 500) * quantity

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
            order_items_data_temp.append(
                {"product_id": product_id, "quantity": quantity}
            )

        state = utils.get_user_state_at_date(user_id, created_at, user_locs_map)
        freight_value = utils.calculate_freight_value(
            state, shipping_method, total_weight_g
        )

        order_details_table.append(
            {
                "order_id": order_id,
                "order_channel": random.choice(["WEB", "MOBILE_APP", "MARKETPLACE"]),
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
                "has_discount": Order_discount_rate > 0,
                "items": order_items_data_temp,
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
