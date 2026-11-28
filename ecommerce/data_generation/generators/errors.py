"""Controlled fault injection for testing Silver-layer data quality rules.

Mutates generated dataset tables with configurable rates of missing
keys, malformed contact formats, out-of-range timestamps, negative numeric
values, and broken SCD Type 2 timelines.
"""

from datetime import datetime, timedelta
import random

import config

INCORRECT_PAST_DATE = datetime(1900, 1, 1, 0, 0, 0)
INCORRECT_FUTURE_DATE = datetime(2099, 12, 31, 23, 59, 59)
ORPHAN_CATEGORY_ID = "CAT-99999"
INVALID_PAYMENT_TYPE = "UNKNOWN"


def _inject_user_errors(user_contact_table: list[dict]) -> None:
    """Injects malformed email addresses and phone numbers into user contacts.

    Args:
        user_contact_table: User email and phone contact records.
    """
    for row in user_contact_table:
        if random.random() < config.INVALID_EMAILS:
            email_mutations = [
                lambda e: f" {e}",
                lambda e: e.replace(" ", ""),
                lambda e: e.replace("@", "@@", 1),
                lambda e: e.split("@")[0],
                lambda e: e.replace("@", ""),
            ]
            row["email"] = random.choice(email_mutations)(row["email"])
        if random.random() < config.INVALID_PHONE_NUMBERS:
            phone_mutations = [
                lambda p: f" {p}",
                lambda p: p.replace("-", ""),
                lambda p: "123",
                lambda p: "not-a-phone",
            ]
            row["phone"] = random.choice(phone_mutations)(row["phone"])


def _inject_order_errors(
    orders_table: list[dict],
    order_items_table: list[dict],
    order_item_pricing_table: list[dict],
    order_status_history_table: list[dict],
) -> None:
    """Injects null foreign keys, sentinel dates, negative amounts, and timeline swaps.

    Args:
        orders_table: Order header records.
        order_items_table: Order line item records.
        order_item_pricing_table: Order item price and discount records.
        order_status_history_table: Order status history records.
    """
    for row in orders_table:
        if random.random() < config.NULL_RATE:
            row["user_id"] = None
        if random.random() < config.WRONG_DATE_RATE:
            row["created_at"] = (
                INCORRECT_PAST_DATE
                if random.choice([True, False])
                else INCORRECT_FUTURE_DATE
            )

    for row in order_items_table:
        if random.random() < config.NULL_RATE:
            row["product_id"] = None

    for row in order_item_pricing_table:
        if random.random() < config.WRONG_PRICE_RATE:
            row["unit_price"] = -abs(row["unit_price"])
        if random.random() < config.WRONG_PRICE_RATE:
            row["discount"] = -abs(row["discount"])

    for i in range(len(order_status_history_table) - 1):
        if (
            order_status_history_table[i]["order_id"]
            == order_status_history_table[i + 1]["order_id"]
        ):
            if random.random() < config.WRONG_DATE_RATE:
                (
                    order_status_history_table[i]["valid_from"],
                    order_status_history_table[i + 1]["valid_from"],
                ) = (
                    order_status_history_table[i + 1]["valid_from"],
                    order_status_history_table[i]["valid_from"],
                )


def _inject_product_errors(
    categories_table: list[dict],
    product_catalog_table: list[dict],
    product_inventory_table: list[dict],
    product_pricing_history_table: list[dict],
) -> None:
    """Injects orphan categories, missing fields, negative values, and SCD2 gaps.

    Args:
        categories_table: Product category hierarchy records.
        product_catalog_table: Product catalog records.
        product_inventory_table: Product stock and weight records.
        product_pricing_history_table: Product price history SCD2 records.
    """
    if len(categories_table) > 5 and random.random() < config.CATEGORY_ORPHAN_RATE:
        categories_table[5]["parent_category_id"] = ORPHAN_CATEGORY_ID

    for row in product_catalog_table:
        if random.random() < config.PRODUCT_MISSING_CAT_RATE:
            row["category_id"] = None
        if row["is_active"] == 1 and random.random() < config.PRODUCT_MISSING_NAME_RATE:
            row["product_name"] = ""

    for row in product_inventory_table:
        if random.random() < config.INVENTORY_NEGATIVE_STOCK_RATE:
            row["stock_quantity"] = -abs(row["stock_quantity"])
        if random.random() < config.INVENTORY_NEGATIVE_DIMENSION_RATE:
            row["weight_g"] = -abs(row["weight_g"])

    for row in product_pricing_history_table:
        if random.random() < config.PRICING_NEGATIVE_PRICE_RATE:
            row["price"] = -abs(row["price"])
        if (
            row["is_current"] == 0
            and random.random() < config.PRICING_SCD2_GAP_RATE
            and row["valid_to"] is not None
        ):
            row["valid_to"] = row["valid_to"] - timedelta(days=2)


def _inject_payment_errors(
    payment_details_table: list[dict],
    payment_status_history_table: list[dict],
) -> None:
    """Injects negative values, invalid payment attributes, and status anomalies.

    Args:
        payment_details_table: Order payment transaction records.
        payment_status_history_table: Payment status transition history records.
    """
    for row in payment_details_table:
        if random.random() < config.PAYMENT_NEGATIVE_VALUE_RATE:
            row["payment_value"] = -abs(row["payment_value"])
        if random.random() < config.PAYMENT_INVALID_TYPE_RATE:
            row["payment_type"] = INVALID_PAYMENT_TYPE
        if (
            random.random() < config.PAYMENT_INVALID_INSTALLMENTS_RATE
            and row["payment_type"] == "VOUCHER"
        ):
            row["payment_installments"] = 12
        if (
            random.random() < config.PAYMENT_DUPLICATE_SEQ_RATE
            and row["payment_sequential"] == 2
        ):
            row["payment_sequential"] = 1

    for i in range(len(payment_status_history_table) - 1):
        curr_log = payment_status_history_table[i]
        next_log = payment_status_history_table[i + 1]
        if curr_log["payment_id"] == next_log["payment_id"]:
            if random.random() < config.PAYMENT_STATUS_ANOMALY_RATE:
                curr_log["is_current"] = 1
            if random.random() < config.PAYMENT_STATUS_ANOMALY_RATE:
                curr_log["valid_from"], next_log["valid_from"] = (
                    next_log["valid_from"],
                    curr_log["valid_from"],
                )


def inject_incorrect_data(
    orders_table: list[dict],
    order_items_table: list[dict],
    order_item_pricing_table: list[dict],
    order_status_history_table: list[dict],
    categories_table: list[dict],
    product_catalog_table: list[dict],
    product_inventory_table: list[dict],
    product_pricing_history_table: list[dict],
    payment_details_table: list[dict],
    payment_status_history_table: list[dict],
    user_contact_table: list[dict],
) -> None:
    """Injects controlled data anomalies across all generated domains.

    Args:
        orders_table: Order header records.
        order_items_table: Order line item records.
        order_item_pricing_table: Order item price and discount records.
        order_status_history_table: Order status history records.
        categories_table: Product category hierarchy records.
        product_catalog_table: Product catalog records.
        product_inventory_table: Product stock and weight records.
        product_pricing_history_table: Product price history SCD2 records.
        payment_details_table: Order payment details records.
        payment_status_history_table: Payment status history records.
        user_contact_table: User email and phone contact records.
    """
    _inject_user_errors(user_contact_table)
    _inject_order_errors(
        orders_table,
        order_items_table,
        order_item_pricing_table,
        order_status_history_table,
    )
    _inject_product_errors(
        categories_table,
        product_catalog_table,
        product_inventory_table,
        product_pricing_history_table,
    )
    _inject_payment_errors(
        payment_details_table,
        payment_status_history_table,
    )
