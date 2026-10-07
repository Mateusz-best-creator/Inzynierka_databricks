import csv
import os
import random
from datetime import datetime, timedelta
from faker import Faker


NUMBER_OF_USERS = 5000
NUMBER_OF_LOCATIONS = 1000
NUMBER_OF_PRODUCTS = 500
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIRECTORY = os.path.join(SCRIPT_DIR, "generated_csv_data")

CURRENCY = "USD"
RANDOM_SEED = 42

# Error injection (Bronze keeps raw data, Silver is expected to clean it).
INJECT_ERRORS = True
ERROR_RATE = 0.01

# Fixed "now" (today at midnight) so that a run with the same seed is
# reproducible during the whole day.
NOW = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)
fake = Faker("en_US")


# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------


def generate_dates(number_of_versions, start=None):
    if number_of_versions <= 0:
        return []

    if start is None:
        years_ago = random.randint(8, 12)
        start = NOW - timedelta(days=365 * years_ago)

    dates = [start]

    for remaining_versions in range(number_of_versions - 1, 0, -1):
        previous_date = dates[-1]

        remaining_days = (NOW - previous_date).days
        max_days = remaining_days - (remaining_versions - 1)

        if max_days <= 1:
            new_date = previous_date + timedelta(days=1)
        else:
            new_date = previous_date + timedelta(days=random.randint(1, max_days))

        dates.append(new_date)

    return dates


def close_history(history):
    # Sets valid_to / is_current for a list of versions ordered by valid_from.
    for i in range(len(history)):
        if i == len(history) - 1:
            history[i]["valid_to"] = None
            history[i]["is_current"] = 1
        else:
            history[i]["valid_to"] = history[i + 1]["valid_from"]
            history[i]["is_current"] = 0

    return history


def next_id(table):
    # 0000000002
    return str(len(table) + 1).zfill(10)


def choose_weighted(options):
    # options: list of (value, weight)
    values = [option[0] for option in options]
    weights = [option[1] for option in options]
    return random.choices(values, weights=weights)[0]


def find_record(history, status):
    for record in history:
        if record["status"] == status:
            return record
    return None


def group_by(rows, key):
    grouped = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(row)
    return grouped


# ---------------------------------------------------------------------------
# Users (existing part)
# ---------------------------------------------------------------------------

# For each combination of transitions, let's generate a few of possible descriptions.
STATUS_DESCRIPTIONS = {
    ("ACTIVE", "INACTIVE"): [
        "User voluntarily deactivated the account.",
        "User temporarily disabled the account.",
        "Account was deactivated at the user's request.",
        "User requested temporary account deactivation.",
    ],
    ("INACTIVE", "ACTIVE"): [
        "User reactivated the account.",
        "User returned and reactivated the account.",
        "Account was reactivated at the user's request.",
        "User restored the previously deactivated account.",
    ],
    ("ACTIVE", "SUSPENDED"): [
        "Account temporarily suspended due to suspicious activity.",
        "Account suspended following a policy violation.",
        "Account temporarily restricted pending a security review.",
        "Account suspended due to unusual account activity.",
    ],
    ("SUSPENDED", "ACTIVE"): [
        "Account suspension was lifted after review.",
        "Security review completed and account restored.",
        "Temporary suspension expired and account was reactivated.",
        "Account restored after the issue was resolved.",
    ],
    ("ACTIVE", "BANNED"): [
        "Account permanently banned due to a serious policy violation.",
        "Account permanently banned following repeated policy violations.",
        "Account banned due to confirmed fraudulent activity.",
        "Account permanently restricted due to severe abuse.",
    ],
    ("SUSPENDED", "BANNED"): [
        "Temporary suspension escalated to a permanent ban.",
        "Account permanently banned following a failed policy review.",
        "Account banned after repeated violations during suspension.",
        "Suspended account permanently banned due to severe policy violations.",
    ],
    ("ACTIVE", "DELETED"): [
        "User permanently deleted the account.",
        "Account permanently deleted at the user's request.",
        "User requested permanent account deletion.",
        "Account was closed and permanently deleted.",
    ],
    ("INACTIVE", "DELETED"): [
        "Inactive account was permanently deleted at the user's request.",
        "User permanently deleted the previously inactive account.",
        "Inactive account was closed and permanently deleted.",
    ],
    ("SUSPENDED", "DELETED"): [
        "User permanently deleted the suspended account.",
        "Suspended account was permanently deleted at the user's request.",
        "Account was closed following the suspension.",
    ],
}


def choose_next_status(current_status):
    if current_status in ("BANNED", "DELETED"):
        return None

    if current_status == "ACTIVE":
        choices = [
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "INACTIVE",
            "INACTIVE",
            "SUSPENDED",
            "BANNED",
            "DELETED",
            "DELETED",
        ]

    elif current_status == "INACTIVE":
        choices = [
            "INACTIVE",
            "INACTIVE",
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "DELETED",
        ]

    elif current_status == "SUSPENDED":
        choices = [
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "SUSPENDED",
            "SUSPENDED",
            "BANNED",
            "DELETED",
            "DELETED",
        ]

    else:
        return None

    return random.choice(choices)


def get_status_description(previous_status, new_status):
    descriptions = STATUS_DESCRIPTIONS.get((previous_status, new_status))

    if descriptions:
        return random.choice(descriptions)

    if previous_status == new_status:
        if new_status == "ACTIVE":
            return "Account remained active."

        if new_status == "INACTIVE":
            return "Account remained inactive."

        if new_status == "SUSPENDED":
            return "Account remained suspended."

        if new_status == "BANNED":
            return "Account remained permanently banned."

        if new_status == "DELETED":
            return "Account remained permanently deleted."

    return f"Account status changed from {previous_status} to {new_status}."


def generate_status_history(number_of_versions, start=None):
    if number_of_versions <= 0:
        return []

    dates = generate_dates(number_of_versions, start)

    history = []

    current_status = "ACTIVE"

    history.append(
        {
            "status": "ACTIVE",
            "status_change_description": "Creation of the account.",
            "valid_from": dates[0],
        }
    )

    for i in range(1, len(dates)):
        previous_status = current_status

        next_status = choose_next_status(current_status)

        if next_status is None:
            break

        current_status = next_status

        description = get_status_description(previous_status, current_status)

        history.append(
            {
                "status": current_status,
                "status_change_description": description,
                "valid_from": dates[i],
            }
        )

        if current_status in ("BANNED", "DELETED"):
            break

    return close_history(history)


def generate_locations(number_of_locations):
    location_table = []

    for i in range(number_of_locations):
        location_table.append(
            {
                "location_id": str(i + 1),
                "city": fake.city(),
                "state": fake.state(),
                "zip_code": fake.zipcode(),
                "country": "United States",
            }
        )

    return location_table


def generate_number_of_location_versions():
    choice = random.randint(0, 10)

    if choice <= 4:
        return 1
    elif choice <= 6:
        return 2
    elif choice <= 8:
        return 3
    else:
        return 4


def generate_number_of_status_versions():
    choice = random.randint(0, 10)

    if choice <= 4:
        return 1
    elif choice <= 6:
        return 2
    elif choice <= 8:
        return 3
    else:
        return 4


def generate_user_tables():
    users_table = []
    user_profile_table = []
    user_contact_table = []
    user_status_history_table = []
    user_location_table = []

    location_table = generate_locations(NUMBER_OF_LOCATIONS)

    for i in range(NUMBER_OF_USERS):
        # 0000000002
        user_id = str(i + 1).zfill(10)
        customer_unique_id = f"CUST-{user_id}"

        # The account is created 8-12 years ago. Status history and location
        # history both start at this moment, so all dates stay consistent.
        created_at = (
            NOW
            - timedelta(days=random.randint(365 * 8, 365 * 12))
            + timedelta(seconds=random.randint(0, 86399))
        )

        users_table.append(
            {
                "user_id": user_id,
                "customer_unique_id": customer_unique_id,
                "created_at": created_at,
            }
        )

        choice = random.choice([0, 1])

        if i > 0 and i % 1000 == 0:
            gender = "NOT_SPECIFIED"
        else:
            gender = "MALE" if choice == 0 else "FEMALE"

        if gender == "MALE":
            first_name = fake.first_name_male()
            last_name = fake.last_name_male()
        elif gender == "FEMALE":
            first_name = fake.first_name_female()
            last_name = fake.last_name_female()
        else:
            # For NOT_SPECIFIED option we will generate generic names
            first_name = fake.first_name()
            last_name = fake.last_name()

        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)

        user_profile_table.append(
            {
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "birth_date": birth_date,
                "gender": gender,
            }
        )

        user_contact_table.append(
            {
                "user_id": user_id,
                "email": fake.email(),
                "phone": fake.phone_number(),
            }
        )

        number_of_location_versions = generate_number_of_location_versions()
        location_ids = random.sample(
            range(1, NUMBER_OF_LOCATIONS + 1), number_of_location_versions
        )
        location_dates = generate_dates(number_of_location_versions, created_at)

        for j in range(len(location_dates)):
            # The last date is treated as current
            if j == len(location_dates) - 1:
                is_current = 1
                valid_to = None
            else:
                is_current = 0
                valid_to = location_dates[j + 1]

            current_length = len(user_location_table)
            user_location_table.append(
                {
                    "id": current_length + 1,
                    "user_id": user_id,
                    "location_id": location_ids[j],
                    "valid_from": location_dates[j],
                    "valid_to": valid_to,
                    "is_current": is_current,
                }
            )

        number_of_status_versions = generate_number_of_status_versions()
        status_history = generate_status_history(number_of_status_versions, created_at)

        for status_record in status_history:
            user_status_history_table.append(
                {
                    "id": next_id(user_status_history_table),
                    "user_id": user_id,
                    "status": status_record["status"],
                    "status_change_description": (
                        status_record["status_change_description"]
                    ),
                    "valid_from": status_record["valid_from"],
                    "valid_to": status_record["valid_to"],
                    "is_current": status_record["is_current"],
                }
            )

    return {
        "users": users_table,
        "user_profile": user_profile_table,
        "user_contact": user_contact_table,
        "user_location": user_location_table,
        "location": location_table,
        "user_status_history": user_status_history_table,
    }


# ---------------------------------------------------------------------------
# Reference tables: categories, products, payment types
# ---------------------------------------------------------------------------

# (category_name, parent_category_name, product nouns used for leaf categories)
CATEGORY_DEFINITIONS = [
    ("Electronics", None, []),
    ("Smartphones", "Electronics", ["Phone", "Charger", "Phone Case", "Earbuds"]),
    ("Laptops", "Electronics", ["Laptop", "Laptop Bag", "Docking Station"]),
    ("Home & Garden", None, []),
    ("Kitchen", "Home & Garden", ["Blender", "Pan", "Knife Set", "Kettle"]),
    ("Furniture", "Home & Garden", ["Chair", "Desk", "Bookshelf", "Lamp"]),
    ("Fashion", None, []),
    ("Shoes", "Fashion", ["Sneakers", "Boots", "Sandals"]),
    ("Clothing", "Fashion", ["Jacket", "T-Shirt", "Jeans", "Sweater"]),
    ("Sports & Outdoors", None, []),
    ("Fitness", "Sports & Outdoors", ["Dumbbell Set", "Yoga Mat", "Resistance Band"]),
    ("Camping", "Sports & Outdoors", ["Tent", "Backpack", "Sleeping Bag"]),
    ("Books", None, []),
    ("Fiction", "Books", ["Novel", "Short Stories", "Thriller"]),
    ("Non-fiction", "Books", ["Biography", "Guide", "Cookbook"]),
]


def generate_categories():
    categories_table = []
    category_ids = {}
    leaf_categories = []

    for name, parent_name, nouns in CATEGORY_DEFINITIONS:
        category_id = next_id(categories_table)
        category_ids[name] = category_id

        categories_table.append(
            {
                "category_id": category_id,
                "category_name": name,
                "parent_category_id": category_ids[parent_name] if parent_name else None,
            }
        )

        if nouns:
            leaf_categories.append({"category_id": category_id, "nouns": nouns})

    return categories_table, leaf_categories


def generate_products(number_of_products, leaf_categories):
    products_table = []

    for _ in range(number_of_products):
        category = random.choice(leaf_categories)
        noun = random.choice(category["nouns"])

        # Log-normal distribution: many cheap products, a few expensive ones
        price_cents = int(min(max(random.lognormvariate(4.0, 1.0), 5), 3000) * 100)

        products_table.append(
            {
                "product_id": next_id(products_table),
                "product_name": f"{fake.word().capitalize()} {noun}",
                "category_id": category["category_id"],
                "base_price": price_cents / 100,
            }
        )

    return products_table


# (payment_type_name, is_online)
PAYMENT_TYPE_DEFINITIONS = [
    ("CREDIT_CARD", 1),
    ("DEBIT_CARD", 1),
    ("BLIK", 1),
    ("BANK_TRANSFER", 1),
    ("VOUCHER", 1),
    ("CASH_ON_DELIVERY", 0),
]


def generate_payment_types():
    payment_type_table = []

    for name, is_online in PAYMENT_TYPE_DEFINITIONS:
        payment_type_table.append(
            {
                "payment_type_id": next_id(payment_type_table),
                "payment_type_name": name,
                "is_online": is_online,
            }
        )

    return payment_type_table


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

# status -> list of (next_status, weight). None means "the order stays here".
ORDER_STATUS_TRANSITIONS = {
    "CREATED": [("PAID", 85), ("CANCELLED", 15)],
    "PAID": [("SHIPPED", 92), ("CANCELLED", 8)],
    "SHIPPED": [("DELIVERED", 1)],
    "DELIVERED": [("RETURNED", 8), (None, 92)],
}

# (previous, new) -> (min_seconds, max_seconds) between the two statuses
ORDER_TRANSITION_DELAYS = {
    ("CREATED", "PAID"): (180, 2 * 86400),
    ("CREATED", "CANCELLED"): (3600, 3 * 86400),
    ("PAID", "SHIPPED"): (86400, 5 * 86400),
    ("PAID", "CANCELLED"): (3600, 3 * 86400),
    ("SHIPPED", "DELIVERED"): (2 * 86400, 10 * 86400),
    ("DELIVERED", "RETURNED"): (86400, 14 * 86400),
}

ORDER_STATUS_DESCRIPTIONS = {
    (None, "CREATED"): [
        "Order placed by the customer.",
        "Order created and awaiting payment.",
    ],
    ("CREATED", "PAID"): [
        "Payment received for the order.",
        "Order paid and confirmed.",
        "Payment confirmed, order accepted for fulfilment.",
    ],
    ("CREATED", "CANCELLED"): [
        "Order cancelled before payment.",
        "Customer cancelled the order before paying.",
        "Order cancelled automatically due to missing payment.",
    ],
    ("PAID", "SHIPPED"): [
        "Order handed over to the carrier.",
        "Order packed and shipped.",
        "Shipment dispatched from the warehouse.",
    ],
    ("PAID", "CANCELLED"): [
        "Order cancelled at the customer's request after payment.",
        "Order cancelled because the items were out of stock.",
        "Order cancelled after a failed fulfilment check.",
    ],
    ("SHIPPED", "DELIVERED"): [
        "Order delivered to the customer.",
        "Shipment delivered successfully.",
        "Package received by the customer.",
    ],
    ("DELIVERED", "RETURNED"): [
        "Customer returned the order.",
        "Order returned within the return period.",
        "Return accepted after delivery.",
    ],
}

ORDER_CHANNELS = [("WEB", 55), ("MOBILE_APP", 40), ("CALL_CENTER", 5)]
SHIPPING_METHODS = [("STANDARD", 70), ("EXPRESS", 20), ("PICKUP", 10)]


def generate_number_of_orders():
    return random.choices(
        [0, 1, 2, 3, 5, 8, 12],
        weights=[15, 25, 22, 16, 12, 7, 3],
    )[0]


def get_active_windows(status_history):
    # Periods in which the user was ACTIVE (and therefore could place orders).
    windows = []

    for record in status_history:
        if record["status"] != "ACTIVE":
            continue

        end = record["valid_to"] if record["valid_to"] is not None else NOW

        if end > record["valid_from"]:
            windows.append((record["valid_from"], end))

    return windows


def generate_order_timestamp(active_windows):
    weights = [(end - start).total_seconds() for start, end in active_windows]
    start, end = random.choices(active_windows, weights=weights)[0]

    span = (end - start).total_seconds()

    return (start + timedelta(seconds=random.uniform(0, span))).replace(microsecond=0)


def get_order_status_description(previous_status, new_status):
    return random.choice(ORDER_STATUS_DESCRIPTIONS[(previous_status, new_status)])


def generate_order_status_history(purchase_time):
    history = [
        {
            "status": "CREATED",
            "status_change_description": get_order_status_description(None, "CREATED"),
            "valid_from": purchase_time,
        }
    ]

    current_status = "CREATED"

    while current_status in ORDER_STATUS_TRANSITIONS:
        next_status = choose_weighted(ORDER_STATUS_TRANSITIONS[current_status])

        if next_status is None:
            break

        min_seconds, max_seconds = ORDER_TRANSITION_DELAYS[(current_status, next_status)]
        change_time = history[-1]["valid_from"] + timedelta(
            seconds=random.randint(min_seconds, max_seconds)
        )

        # The order has not reached this stage yet.
        if change_time > NOW:
            break

        history.append(
            {
                "status": next_status,
                "status_change_description": get_order_status_description(
                    current_status, next_status
                ),
                "valid_from": change_time,
            }
        )

        current_status = next_status

    return close_history(history)


def find_location_at(location_history, moment):
    for row in location_history:
        end = row["valid_to"] if row["valid_to"] is not None else NOW

        if row["valid_from"] <= moment < end:
            return row["location_id"]

    return location_history[0]["location_id"]


def generate_order_locations(
    order_id, purchase_time, status_history, user_locations, order_location_table
):
    # Delivery address: usually the current address of the user, sometimes
    # a different one. Sometimes the address is changed before shipping.
    if random.random() < 0.85:
        location_id = find_location_at(user_locations, purchase_time)
    else:
        location_id = random.randint(1, NUMBER_OF_LOCATIONS)

    versions = [{"location_id": location_id, "valid_from": purchase_time}]

    stage_record = None
    for record in status_history:
        if record["status"] in ("SHIPPED", "CANCELLED"):
            stage_record = record
            break

    limit = stage_record["valid_from"] if stage_record else NOW
    can_change = (
        (stage_record is None or stage_record["status"] == "SHIPPED")
        and (limit - purchase_time).total_seconds() > 120
    )

    if can_change and random.random() < 0.05:
        new_location_id = random.randint(1, NUMBER_OF_LOCATIONS)

        if new_location_id != location_id:
            seconds = int((limit - purchase_time).total_seconds())
            change_time = purchase_time + timedelta(seconds=random.randint(60, seconds - 1))
            versions.append({"location_id": new_location_id, "valid_from": change_time})

    close_history(versions)

    for version in versions:
        order_location_table.append(
            {
                "id": next_id(order_location_table),
                "order_id": order_id,
                "location_id": version["location_id"],
                "valid_from": version["valid_from"],
                "valid_to": version["valid_to"],
                "is_current": version["is_current"],
            }
        )


# ---------------------------------------------------------------------------
# Order items
# ---------------------------------------------------------------------------


def generate_order_items(
    order_id,
    shipping_method,
    products_table,
    order_items_table,
    order_item_pricing_table,
):
    # Returns the total value of the order in cents:
    # sum(quantity * unit_price - discount + freight_value)
    number_of_items = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 13, 8, 4])[0]
    chosen_products = random.sample(products_table, number_of_items)

    total_cents = 0

    for sequence, product in enumerate(chosen_products, start=1):
        quantity = random.choices([1, 2, 3, 4], weights=[70, 18, 8, 4])[0]

        # Price at the moment of purchase is close to the product base price
        base_cents = round(product["base_price"] * 100)
        unit_cents = max(1, round(base_cents * random.uniform(0.9, 1.1)))
        line_cents = unit_cents * quantity

        if random.random() < 0.25:
            discount_cents = round(line_cents * random.choice([0.05, 0.10, 0.15, 0.20]))
        else:
            discount_cents = 0

        if shipping_method == "PICKUP":
            freight_cents = 0
        else:
            freight_cents = random.randint(300, 1500)
            if shipping_method == "EXPRESS":
                freight_cents = round(freight_cents * 1.8)

        order_item_id = next_id(order_items_table)

        order_items_table.append(
            {
                "order_item_id": order_item_id,
                "order_id": order_id,
                "order_item_seq": sequence,
                "product_id": product["product_id"],
                "quantity": quantity,
            }
        )

        order_item_pricing_table.append(
            {
                "order_item_id": order_item_id,
                "unit_price": unit_cents / 100,
                "discount": discount_cents / 100,
                "freight_value": freight_cents / 100,
            }
        )

        total_cents += line_cents - discount_cents + freight_cents

    return total_cents


# ---------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------

PAYMENT_STATUS_DESCRIPTIONS = {
    (None, "PENDING"): [
        "Payment initiated.",
        "Payment created and awaiting confirmation.",
    ],
    ("PENDING", "PAID"): [
        "Payment confirmed.",
        "Funds received successfully.",
        "Payment settled.",
    ],
    ("PENDING", "FAILED"): [
        "Payment not completed because the order was cancelled.",
        "Payment voided after order cancellation.",
    ],
    ("PAID", "REFUNDED"): [
        "Payment refunded to the customer.",
        "Refund issued after order cancellation or return.",
        "Funds returned to the original payment method.",
    ],
}

SINGLE_PAYMENT_TYPES = [
    ("CREDIT_CARD", 45),
    ("DEBIT_CARD", 20),
    ("BLIK", 15),
    ("BANK_TRANSFER", 12),
    ("CASH_ON_DELIVERY", 8),
]
SPLIT_PAYMENT_TYPES = ["CREDIT_CARD", "DEBIT_CARD", "BLIK", "BANK_TRANSFER"]


def split_payment_amounts(total_cents):
    # Returns a list of (payment_type_name, value_in_cents) that sums to total.
    if total_cents >= 2000 and random.random() < 0.12:
        voucher_cents = round(total_cents * random.uniform(0.1, 0.5))
        other_type = random.choice(SPLIT_PAYMENT_TYPES)
        return [("VOUCHER", voucher_cents), (other_type, total_cents - voucher_cents)]

    return [(choose_weighted(SINGLE_PAYMENT_TYPES), total_cents)]


def generate_installments(payment_type_name):
    if payment_type_name != "CREDIT_CARD":
        return 1

    return random.choices([1, 2, 3, 6, 10, 12], weights=[50, 15, 12, 10, 8, 5])[0]


def get_payment_status_description(previous_status, new_status):
    return random.choice(PAYMENT_STATUS_DESCRIPTIONS[(previous_status, new_status)])


def generate_payment_status_history(payment_type_name, created_time, order_history):
    # The payment history follows the order history:
    # - paid when the order is PAID (cash on delivery: when DELIVERED),
    # - refunded when a paid order is cancelled or returned,
    # - failed when the order was cancelled before the payment.
    history = [
        {
            "status": "PENDING",
            "status_change_description": get_payment_status_description(None, "PENDING"),
            "valid_from": created_time,
        }
    ]

    paid_step = "DELIVERED" if payment_type_name == "CASH_ON_DELIVERY" else "PAID"
    paid_record = find_record(order_history, paid_step)
    cancelled_record = find_record(order_history, "CANCELLED")
    returned_record = find_record(order_history, "RETURNED")

    if paid_record:
        paid_time = paid_record["valid_from"] + timedelta(seconds=random.randint(0, 60))
        if paid_time > NOW:
            paid_time = paid_record["valid_from"]

        history.append(
            {
                "status": "PAID",
                "status_change_description": get_payment_status_description(
                    "PENDING", "PAID"
                ),
                "valid_from": paid_time,
            }
        )

        refund_record = cancelled_record or returned_record

        if refund_record:
            refund_time = refund_record["valid_from"] + timedelta(
                seconds=random.randint(3600, 2 * 86400)
            )

            if refund_time <= NOW:
                history.append(
                    {
                        "status": "REFUNDED",
                        "status_change_description": get_payment_status_description(
                            "PAID", "REFUNDED"
                        ),
                        "valid_from": refund_time,
                    }
                )

    elif cancelled_record:
        history.append(
            {
                "status": "FAILED",
                "status_change_description": get_payment_status_description(
                    "PENDING", "FAILED"
                ),
                "valid_from": cancelled_record["valid_from"],
            }
        )

    return close_history(history)


def generate_payments(
    order_id,
    purchase_time,
    total_cents,
    order_history,
    payment_type_ids,
    payments_table,
    payment_details_table,
    payment_status_history_table,
):
    for sequence, (type_name, value_cents) in enumerate(
        split_payment_amounts(total_cents), start=1
    ):
        payment_id = next_id(payments_table)

        created_time = min(
            purchase_time + timedelta(seconds=random.randint(5, 120)), NOW
        )

        payments_table.append(
            {
                "payment_id": payment_id,
                "order_id": order_id,
                "payment_sequential": sequence,
                "created_at": created_time,
            }
        )

        payment_details_table.append(
            {
                "payment_id": payment_id,
                "payment_type_id": payment_type_ids[type_name],
                "payment_installments": generate_installments(type_name),
                "payment_value": value_cents / 100,
                "currency": CURRENCY,
            }
        )

        history = generate_payment_status_history(type_name, created_time, order_history)

        for record in history:
            payment_status_history_table.append(
                {
                    "id": next_id(payment_status_history_table),
                    "payment_id": payment_id,
                    "status": record["status"],
                    "status_change_description": record["status_change_description"],
                    "valid_from": record["valid_from"],
                    "valid_to": record["valid_to"],
                    "is_current": record["is_current"],
                }
            )


# ---------------------------------------------------------------------------
# Orders + order items + payments
# ---------------------------------------------------------------------------


def generate_order_tables(user_tables, products_table, payment_type_table):
    orders_table = []
    order_details_table = []
    order_status_history_table = []
    order_location_table = []
    order_items_table = []
    order_item_pricing_table = []
    payments_table = []
    payment_details_table = []
    payment_status_history_table = []

    status_by_user = group_by(user_tables["user_status_history"], "user_id")
    locations_by_user = group_by(user_tables["user_location"], "user_id")
    payment_type_ids = {
        row["payment_type_name"]: row["payment_type_id"] for row in payment_type_table
    }

    for user in user_tables["users"]:
        user_id = user["user_id"]

        # Orders can only be placed while the user is ACTIVE.
        active_windows = get_active_windows(status_by_user[user_id])

        if not active_windows:
            continue

        for _ in range(generate_number_of_orders()):
            purchase_time = generate_order_timestamp(active_windows)
            order_id = next_id(orders_table)

            orders_table.append(
                {
                    "order_id": order_id,
                    "user_id": user_id,
                    "created_at": purchase_time,
                }
            )

            shipping_method = choose_weighted(SHIPPING_METHODS)

            order_details_table.append(
                {
                    "order_id": order_id,
                    "order_channel": choose_weighted(ORDER_CHANNELS),
                    "shipping_method": shipping_method,
                    "currency": CURRENCY,
                }
            )

            status_history = generate_order_status_history(purchase_time)

            for record in status_history:
                order_status_history_table.append(
                    {
                        "id": next_id(order_status_history_table),
                        "order_id": order_id,
                        "status": record["status"],
                        "status_change_description": record[
                            "status_change_description"
                        ],
                        "valid_from": record["valid_from"],
                        "valid_to": record["valid_to"],
                        "is_current": record["is_current"],
                    }
                )

            generate_order_locations(
                order_id,
                purchase_time,
                status_history,
                locations_by_user[user_id],
                order_location_table,
            )

            total_cents = generate_order_items(
                order_id,
                shipping_method,
                products_table,
                order_items_table,
                order_item_pricing_table,
            )

            generate_payments(
                order_id,
                purchase_time,
                total_cents,
                status_history,
                payment_type_ids,
                payments_table,
                payment_details_table,
                payment_status_history_table,
            )

    return {
        "orders": orders_table,
        "order_details": order_details_table,
        "order_status_history": order_status_history_table,
        "order_location": order_location_table,
        "order_items": order_items_table,
        "order_item_pricing": order_item_pricing_table,
        "payments": payments_table,
        "payment_details": payment_details_table,
        "payment_status_history": payment_status_history_table,
    }


# ---------------------------------------------------------------------------
# Error injection (applied after generation, so generation logic stays clean)
# ---------------------------------------------------------------------------


def inject_errors(tables):
    def how_many(rows):
        return max(1, int(len(rows) * ERROR_RATE))

    orders = tables["orders"]
    order_items = tables["order_items"]
    pricing = tables["order_item_pricing"]
    payment_details = tables["payment_details"]

    report = {}

    # 1. Duplicated order_id (exact duplicate row appended to the file)
    duplicates = [dict(row) for row in random.sample(orders, how_many(orders))]
    orders.extend(duplicates)
    report["duplicated order_id"] = len(duplicates)

    # 2. created_at out of range (far in the future or long before the platform)
    broken_orders = random.sample(orders, how_many(orders))
    for row in broken_orders:
        if random.random() < 0.5:
            row["created_at"] = NOW + timedelta(days=random.randint(30, 730))
        else:
            row["created_at"] = datetime(1990, 1, 1) + timedelta(
                days=random.randint(0, 3650)
            )
    report["order created_at out of range"] = len(broken_orders)

    # 3. Negative unit_price
    broken_prices = random.sample(pricing, how_many(pricing))
    for row in broken_prices:
        row["unit_price"] = -abs(row["unit_price"])
    report["negative unit_price"] = len(broken_prices)

    # 4. Orphaned order items (order_id that does not exist in orders)
    orphan_count = how_many(order_items)
    for k in range(orphan_count):
        order_item_id = next_id(order_items)
        order_items.append(
            {
                "order_item_id": order_item_id,
                "order_id": str(9_000_000_000 + k).zfill(10),
                "order_item_seq": 1,
                "product_id": str(random.randint(1, NUMBER_OF_PRODUCTS)).zfill(10),
                "quantity": random.randint(1, 3),
            }
        )
        pricing.append(
            {
                "order_item_id": order_item_id,
                "unit_price": round(random.uniform(5, 500), 2),
                "discount": 0.0,
                "freight_value": round(random.uniform(3, 15), 2),
            }
        )
    report["orphaned order_items"] = orphan_count

    # 5. payment_value that does not match the sum of the order items
    broken_payments = random.sample(payment_details, how_many(payment_details))
    for row in broken_payments:
        row["payment_value"] = round(row["payment_value"] * random.uniform(0.5, 1.5), 2)
    report["payment_value mismatch"] = len(broken_payments)

    return report


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------


def generate_data():
    tables = generate_user_tables()

    categories_table, leaf_categories = generate_categories()
    products_table = generate_products(NUMBER_OF_PRODUCTS, leaf_categories)
    payment_type_table = generate_payment_types()

    tables["categories"] = categories_table
    tables["products"] = products_table
    tables["payment_type"] = payment_type_table

    tables.update(generate_order_tables(tables, products_table, payment_type_table))

    return tables


def format_csv_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float):
        return f"{value:.2f}"
    return value


def write_csv_file(destination, data):
    field_names = data[0].keys()

    with open(destination, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)

        writer.writeheader()

        for row in data:
            formatted_row = {key: format_csv_value(value) for key, value in row.items()}

            writer.writerow(formatted_row)


# table name -> file name
OUTPUT_FILES = {
    "users": "users_table.csv",
    "user_profile": "user_profile_table.csv",
    "user_contact": "user_contact_table.csv",
    "user_location": "user_location_table.csv",
    "location": "location_table.csv",
    "user_status_history": "user_status_history_table.csv",
    "categories": "categories_table.csv",
    "products": "products_table.csv",
    "payment_type": "payment_type_table.csv",
    "orders": "orders_table.csv",
    "order_details": "order_details_table.csv",
    "order_status_history": "order_status_history_table.csv",
    "order_location": "order_location_table.csv",
    "order_items": "order_items_table.csv",
    "order_item_pricing": "order_item_pricing_table.csv",
    "payments": "payments_table.csv",
    "payment_details": "payment_details_table.csv",
    "payment_status_history": "payment_status_history_table.csv",
}


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    all_tables = generate_data()

    error_report = inject_errors(all_tables) if INJECT_ERRORS else {}

    for table_name, file_name in OUTPUT_FILES.items():
        write_csv_file(f"{OUTPUT_DIRECTORY}/{file_name}", all_tables[table_name])

    for table_name in OUTPUT_FILES:
        print(f"{table_name}: {len(all_tables[table_name])}")

    if error_report:
        print("Injected errors:")
        for error_name, count in error_report.items():
            print(f"  {error_name}: {count}")

    print("Pliki zapisano w:", os.path.abspath(OUTPUT_DIRECTORY))