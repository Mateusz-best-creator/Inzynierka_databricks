import csv
import json
import os
import random
from datetime import datetime, timedelta, date
from faker import Faker
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIRECTORY = os.path.join(SCRIPT_DIR, "generated_combined_data")

# -----------------------------------------------------------------------------
# GLOBAL CONFIGURATION
# -----------------------------------------------------------------------------
NUMBER_OF_USERS = 5000
NUMBER_OF_LOCATIONS = 1000
NUMBER_OF_PRODUCTS = 1000
NUMBER_OF_CATEGORIES = 20
NUMBER_OF_ORDERS = 15000
NUMBER_OF_GENERAL_SESSIONS = 15000  # Additional non-purchase sessions

# Error Rates
NULL_RATE = 0#0.2
WRONG_PRICE_RATE =  0#0.1
WRONG_DATE_RATE =  0#0.3

INVALID_EMAILS =  0#0.1
INVALID_PHONE_NUMBERS =  0#0.1

PRODUCT_MISSING_CAT_RATE =  0#0.05
PRODUCT_MISSING_NAME_RATE =  0#0.05
INVENTORY_NEGATIVE_STOCK_RATE =  0#0.05
INVENTORY_NEGATIVE_DIMENSION_RATE =  0#0.05
PRICING_NEGATIVE_PRICE_RATE =  0#0.05
PRICING_SCD2_GAP_RATE =  0#0.02
CATEGORY_ORPHAN_RATE =  0#0.05

PAYMENT_NEGATIVE_VALUE_RATE =  0#0.05
PAYMENT_INVALID_TYPE_RATE =  0#0.03
PAYMENT_INVALID_INSTALLMENTS_RATE =  0#0.02
PAYMENT_DUPLICATE_SEQ_RATE =  0#0.02
PAYMENT_STATUS_ANOMALY_RATE =  0#0.03

DISCONTINUATION_RATE = 0.3

fake = Faker("en_US")

# Master Timeline Anchors to prevent paradoxes
NOW = datetime.now()
FOUNDATION_START = NOW - timedelta(days=365*4)  # 4 years ago
FOUNDATION_END = NOW - timedelta(days=365*3)    # 3 years ago


# -----------------------------------------------------------------------------
# SHARED UTILITIES
# -----------------------------------------------------------------------------
def random_date(start, end):
    delta = end - start
    random_days = random.randrange(delta.days)
    random_seconds = random.randrange(24*60*60)
    return start + timedelta(days=random_days, seconds=random_seconds)


def generate_scd2_timeline(start_date, number_of_versions):
    if number_of_versions <= 0:
        return []
    
    dates = [start_date]
    for _ in range(number_of_versions - 1):
        next_date = dates[-1] + timedelta(days=random.randint(10, 300), hours=random.randint(1, 23))
        if next_date > NOW:
            break
        dates.append(next_date)
    return dates


# =============================================================================
# MATEUSZ MODULE: USERS & LOCATIONS
# =============================================================================
STATUS_DESCRIPTIONS = {
    ("ACTIVE", "INACTIVE"): ["User voluntarily deactivated the account.", "User temporarily disabled the account."],
    ("INACTIVE", "ACTIVE"): ["User reactivated the account.", "User returned and reactivated the account."],
    ("ACTIVE", "SUSPENDED"): ["Account temporarily suspended due to suspicious activity."],
    ("SUSPENDED", "ACTIVE"): ["Account suspension was lifted after review."],
    ("ACTIVE", "BANNED"): ["Account permanently banned due to a serious policy violation."],
    ("SUSPENDED", "BANNED"): ["Temporary suspension escalated to a permanent ban."],
    ("ACTIVE", "DELETED"): ["User permanently deleted the account."],
    ("INACTIVE", "DELETED"): ["Inactive account was permanently deleted at the user's request."],
    ("SUSPENDED", "DELETED"): ["User permanently deleted the suspended account."],
}


def choose_next_status(current_status):
    if current_status in ("BANNED", "DELETED"):
        return None

    if current_status == "ACTIVE":
        choices = ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE", "SUSPENDED", "BANNED", "DELETED"]
    elif current_status == "INACTIVE":
        choices = ["INACTIVE", "ACTIVE", "DELETED"]
    elif current_status == "SUSPENDED":
        choices = ["ACTIVE", "SUSPENDED", "BANNED", "DELETED"]
    else:
        return None
    return random.choice(choices)


def get_status_description(previous_status, new_status):
    descriptions = STATUS_DESCRIPTIONS.get((previous_status, new_status))
    if descriptions:
        return random.choice(descriptions)
    if previous_status == new_status:
        return f"Account remained {new_status.lower()}."
    return f"Account status changed from {previous_status} to {new_status}."


def generate_status_history(created_at, number_of_versions):
    if number_of_versions <= 0:
        return []

    dates = generate_scd2_timeline(created_at, number_of_versions)
    history = []
    current_status = "ACTIVE"

    # Rule: Every user account starts with status ACTIVE upon creation
    history.append({
        "status": "ACTIVE",
        "status_change_description": "Creation of the account.",
        "valid_from": dates[0],
    })

    for i in range(1, len(dates)):
        previous_status = current_status
        next_status = choose_next_status(current_status)

        if next_status is None:
            break

        current_status = next_status
        description = get_status_description(previous_status, current_status)

        history.append({
            "status": current_status,
            "status_change_description": description,
            "valid_from": dates[i],
        })

        if current_status in ("BANNED", "DELETED"):
            break

    for i in range(len(history)):
        if i == len(history) - 1:
            history[i]["valid_to"] = None
            history[i]["is_current"] = 1
        else:
            history[i]["valid_to"] = history[i + 1]["valid_from"]
            history[i]["is_current"] = 0

    return history


def generate_locations(number_of_locations):
    location_table = []
    for i in range(number_of_locations):
        location_table.append({
            "location_id": str(i + 1),
            "city": fake.city(),
            "state": fake.state(),
            "zip_code": fake.zipcode(),
            "country": "United States",
        })
    return location_table


def generate_data():
    users_table, user_profile_table, user_contact_table = [], [], []
    user_status_history_table, user_location_table = [], []
    location_table = generate_locations(NUMBER_OF_LOCATIONS)

    for i in range(NUMBER_OF_USERS):
        user_id = str(i + 1).zfill(10)
        user_created_at = random_date(FOUNDATION_START, FOUNDATION_END)
        users_table.append({
            "user_id": user_id,
            "customer_unique_id": f"CUST-{user_id}",
            "created_at": user_created_at,
        })

        gender = "NOT_SPECIFIED" if i > 0 and i % 1000 == 0 else ("MALE" if random.choice([0, 1]) == 0 else "FEMALE")
        first_name = fake.first_name_male() if gender == "MALE" else (fake.first_name_female() if gender == "FEMALE" else fake.first_name())
        last_name = fake.last_name_male() if gender == "MALE" else (fake.last_name_female() if gender == "FEMALE" else fake.last_name())

        user_profile_table.append({
            "user_id": user_id,
            "first_name": first_name,
            "last_name": last_name,
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=80),
            "gender": gender,
        })

        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,100)}@gmail.com"
        phone = fake.phone_number()

        user_contact_table.append({"user_id": user_id, "email": email, "phone": phone})

        # 1. Generate status history first (Starts with ACTIVE)
        status_history = generate_status_history(user_created_at, random.randint(1, 4))
        
        # Determine if there is a PERMANENT terminal state (DELETED or BANNED).
        # Note: SUSPENDED is excluded so temporary suspensions don't permanently cut off locations.
        terminal_date = None
        for status_record in status_history:
            if status_record["status"] in ("DELETED", "BANNED"):
                terminal_date = status_record["valid_from"]
                break

        # 2. Generate location history, clipped ONLY if permanently deleted or banned
        num_loc_versions = random.randint(1, 4)
        loc_dates = generate_scd2_timeline(user_created_at, num_loc_versions)
        
        if terminal_date:
            loc_dates = [d for d in loc_dates if d < terminal_date]
            if not loc_dates:
                loc_dates = [user_created_at]

        loc_ids = random.sample(range(1, NUMBER_OF_LOCATIONS + 1), len(loc_dates))

        for j in range(len(loc_dates)):
            is_last = (j == len(loc_dates) - 1)
            
            # If this is the last location and account reached a permanent terminal state, 
            # cap valid_to at terminal_date and keep is_current = 1 as the final stage.
            if is_last and terminal_date:
                valid_to = terminal_date
                is_current = 1
            else:
                is_current = 1 if is_last else 0
                valid_to = None if is_current else loc_dates[j + 1]

            user_location_table.append({
                "id": len(user_location_table) + 1,
                "user_id": user_id,
                "location_id": loc_ids[j],
                "valid_from": loc_dates[j],
                "valid_to": valid_to,
                "is_current": is_current,
            })

        for status_record in status_history:
            user_status_history_table.append({
                "id": str(len(user_status_history_table) + 1).zfill(10),
                "user_id": user_id,
                "status": status_record["status"],
                "status_change_description": status_record["status_change_description"],
                "valid_from": status_record["valid_from"],
                "valid_to": status_record["valid_to"],
                "is_current": status_record["is_current"],
            })

    return users_table, user_profile_table, user_contact_table, user_location_table, location_table, user_status_history_table

# =============================================================================
# MARCIN MODULE: CATEGORIES, PRODUCTS (DECOUPLED TIMELINES)
# =============================================================================
CATEGORY_STRUCTURE = {"Electronics": ["Laptops", "Smartphones", "Audio", "Cameras"], "Home & Garden": ["Furniture", "Kitchen", "Decor", "Lighting"], "Sports": ["Fitness", "Cycling", "Running", "Swimming"], "Clothing": ["Men", "Women", "Kids", "Accessories"]}
CATEGORY_NOUNS = {"Laptops": ["Laptop", "Notebook", "Ultrabook"], "Smartphones": ["Smartphone", "Phone", "Phablet"], "Audio": ["Headphones", "Earbuds", "Speaker"], "Cameras": ["Camera", "DSLR", "Mirrorless"], "Furniture": ["Chair", "Table", "Sofa"], "Kitchen": ["Blender", "Pan", "Knife"], "Decor": ["Vase", "Painting", "Rug"], "Lighting": ["Bulb", "Desk Lamp", "Chandelier"], "Fitness": ["Yoga Mat", "Dumbbells", "Treadmill"], "Cycling": ["Bike", "Helmet", "Gloves"], "Running": ["Shoes", "Shorts", "Bottle"], "Swimming": ["Goggles", "Swimsuit", "Cap"], "Men": ["T-Shirt", "Jeans", "Jacket"], "Women": ["Dress", "Skirt", "Blouse"], "Kids": ["Toy", "T-Shirt", "Pants"], "Accessories": ["Watch", "Belt", "Wallet"]}

def generate_categories_table():
    categories = []
    cat_counter = 1
    cat_mapping = {}

    for main_cat, sub_cats in CATEGORY_STRUCTURE.items():
        main_id = f"CAT-{str(cat_counter).zfill(5)}"
        categories.append({"category_id": main_id, "parent_category_id": None, "category_name": main_cat, "created_at": random_date(FOUNDATION_START, FOUNDATION_END)})
        cat_counter += 1

        for sub_cat in sub_cats:
            sub_id = f"CAT-{str(cat_counter).zfill(5)}"
            cat_mapping[sub_id] = sub_cat
            categories.append({"category_id": sub_id, "parent_category_id": main_id, "category_name": sub_cat, "created_at": random_date(FOUNDATION_START, FOUNDATION_END)})
            cat_counter += 1
    return categories, cat_mapping


def generate_products_domain_tables(number_of_products, cat_mapping):
    catalog, inventory, pricing = [], [], []
    subcat_ids = list(cat_mapping.keys())

    for i in range(1, number_of_products + 1):
        prod_id = f"PRD-{str(i).zfill(10)}"
        cat_id = random.choice(subcat_ids)
        subcat_name = cat_mapping[cat_id]
        created_at = random_date(FOUNDATION_START, FOUNDATION_END)

        product_base_name = f"{fake.word().capitalize()} {random.choice(CATEGORY_NOUNS[subcat_name])} Pro"
        brand = fake.company()

        # ==========================================
        # 1. PRODUCT CATALOG SCD2 TIMELINE
        # ==========================================
        cat_dates = generate_scd2_timeline(created_at, random.randint(1, 3))
        num_cat_versions = len(cat_dates)
        discontinued = random.random() < DISCONTINUATION_RATE and num_cat_versions > 1

        discontinuation_date = None

        for j in range(num_cat_versions):
            is_current = 1 if j == num_cat_versions - 1 else 0
            
            if discontinued and j == num_cat_versions - 1:
                is_active = 0
                description = "Product has been discontinued."
                valid_to = None
                discontinuation_date = cat_dates[j]
            else:
                is_active = 1
                description = fake.paragraph(nb_sentences=2) if j == 0 else "Product details updated."
                valid_to = None if is_current else cat_dates[j + 1]

            catalog.append({
                "id": str(len(catalog) + 1).zfill(10),
                "product_id": prod_id,
                "category_id": cat_id,
                "product_name": product_base_name,
                "brand": brand,
                "description": description,
                "is_active": is_active,
                "valid_from": cat_dates[j],
                "valid_to": valid_to,
                "is_current": is_current
            })
            
            if not is_active:
                break

        # ==========================================
        # 2. INVENTORY SCD2 TIMELINE
        # ==========================================
        inv_dates = generate_scd2_timeline(created_at, random.randint(1, 4))
        
        # If discontinued, filter out any inventory change dates that occur AFTER discontinuation
        if discontinuation_date:
            inv_dates = [d for d in inv_dates if d <= discontinuation_date]

        weight, length, height, width = random.randint(100, 5000), random.randint(10, 100), random.randint(10, 100), random.randint(10, 100)
        stock_qty = random.randint(5, 120)

        for j in range(len(inv_dates)):
            is_last = (j == len(inv_dates) - 1)
            
            if is_last and discontinuation_date:
                # Last entry gets valid_to = discontinuation_date, but remains is_current = 1
                valid_to = discontinuation_date
                is_current = 1
            else:
                is_current = 1 if is_last else 0
                valid_to = None if is_current else inv_dates[j + 1]

            inventory.append({
                "inventory_id": f"INV-{len(inventory) + 1}",
                "product_id": prod_id,
                "weight_g": weight,
                "length_cm": length,
                "height_cm": height,
                "width_cm": width,
                "stock_quantity": stock_qty,
                "valid_from": inv_dates[j],
                "valid_to": valid_to,
                "is_current": is_current
            })

        # ==========================================
        # 3. PRICING SCD2 TIMELINE
        # ==========================================
        pricing_dates = generate_scd2_timeline(created_at, random.randint(1, 4))
        
        if discontinuation_date:
            pricing_dates = [d for d in pricing_dates if d <= discontinuation_date]

        current_price = round(random.uniform(10.0, 800.0), 2)

        for j in range(len(pricing_dates)):
            is_last = (j == len(pricing_dates) - 1)
            
            if is_last and discontinuation_date:
                # Last entry gets valid_to = discontinuation_date, but remains is_current = 1
                valid_to = discontinuation_date
                is_current = 1
            else:
                is_current = 1 if is_last else 0
                valid_to = None if is_current else pricing_dates[j + 1]

            if j > 0:
                current_price = round(max(5.0, current_price * random.uniform(0.85, 1.25)), 2)

            pricing.append({
                "unique_id": f"PRC-{len(pricing) + 1}",
                "product_id": prod_id,
                "price": current_price,
                "cost_price": round(current_price * random.uniform(0.50, 0.80), 2),
                "valid_from": pricing_dates[j],
                "valid_to": valid_to,
                "is_current": is_current
            })

    return catalog, inventory, pricing


def build_product_catalog_lookup(product_catalog):
    catalog_map = {}
    for row in product_catalog:
        catalog_map.setdefault(row["product_id"], []).append(row)
    return catalog_map


def is_product_active_at_date_fast(product_id, order_date, catalog_map):
    if product_id not in catalog_map:
        return True
    for row in catalog_map[product_id]:
        if row["valid_from"] <= order_date:
            if row["valid_to"] is None or order_date < row["valid_to"]:
                return row["is_active"] == 1
    return True


# =============================================================================
# MACIEK MODULE: ORDERS
# =============================================================================
def get_historical_price(product_id, order_date, pricing_table):
    for row in pricing_table:
        if row["product_id"] == product_id:
            if row["valid_from"] <= order_date:
                if row["valid_to"] is None or row["valid_to"] > order_date:
                    return row["price"]
    return 50.0


STATE_BASE_FREIGHT = {
    "California": 10.0, "New York": 9.0, "Washington": 9.0,
    "Texas": 7.0, "Florida": 7.0, "Illinois": 6.0, "DEFAULT": 6.0,
}
FREIGHT_PER_KG = 0.8
SHIPPING_MULTIPLIERS = {"STANDARD": 1.0, "EXPRESS": 1.8, "SAME_DAY": 2.5, "PICKUP": 0.0}


def build_product_catalog_lookup(product_catalog):
    catalog_map = {}
    for row in product_catalog:
        catalog_map.setdefault(row["product_id"], []).append(row)
    return catalog_map


def is_product_active_at_date_fast(product_id, order_date, catalog_map):
    if product_id not in catalog_map:
        return True
    for row in catalog_map[product_id]:
        if row["valid_from"] <= order_date:
            if row["valid_to"] is None or order_date < row["valid_to"]:
                return row["is_active"] == 1
    return True


def build_user_state_lookup(user_location_table, location_table):
    loc_state_map = {loc["location_id"]: loc["state"] for loc in location_table}
    user_locs = {}
    for ul in user_location_table:
        user_locs.setdefault(ul["user_id"], []).append({
            "valid_from": ul["valid_from"],
            "valid_to": ul["valid_to"],
            "state": loc_state_map.get(str(ul["location_id"]), "California"),
        })
    return user_locs


def get_user_state_at_date(user_id, order_date, user_locs_map):
    if user_id not in user_locs_map:
        return "California"
    for period in user_locs_map[user_id]:
        if period["valid_from"] <= order_date and (period["valid_to"] is None or order_date < period["valid_to"]):
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
        if row["valid_from"] <= order_date and (row["valid_to"] is None or order_date < row["valid_to"]):
            # Users cannot place orders if their account is INACTIVE, SUSPENDED, BANNED, or DELETED
            if row["status"] in ("INACTIVE", "SUSPENDED", "BANNED", "DELETED"):
                return False
            return True
            
    # Fallback to the latest status record if timestamp sits past open intervals
    last_status = user_status_map[user_id][-1]["status"]
    return last_status not in ("INACTIVE", "SUSPENDED", "BANNED", "DELETED")


def calculate_freight_value(state, shipping_method, total_weight_g):
    base_cost = STATE_BASE_FREIGHT.get(state, STATE_BASE_FREIGHT["DEFAULT"])
    weight_cost = (total_weight_g / 1000) * FREIGHT_PER_KG
    multiplier = SHIPPING_MULTIPLIERS.get(shipping_method, 1.0)
    return round((base_cost + weight_cost) * multiplier, 2)


def generate_order_status_history(created_at, paid_at, shipping_method):
    processing_at = paid_at + timedelta(hours=random.randint(1, 24))
    shipped_at = processing_at + timedelta(hours=random.randint(12, 72))
    delivery_days = random.randint(1, 2) if shipping_method == "EXPRESS" else random.randint(3, 7)
    delivered_at = shipped_at + timedelta(days=delivery_days, hours=random.randint(0, 12))

    timeline = [
        ("CREATED", created_at),
        ("PENDING_PAYMENT", created_at + timedelta(seconds=random.randint(5, 60))),
        ("PAID", paid_at),
        ("PROCESSING", processing_at),
        ("SHIPPED", shipped_at),
        ("DELIVERED", delivered_at),
    ]
    timeline = [(status, ts) for status, ts in timeline if ts <= NOW]

    history = []
    for i, (status, valid_from) in enumerate(timeline):
        is_current = 1 if i == len(timeline) - 1 else 0
        history.append({
            "status": status, "status_change_description": f"Status updated to {status}",
            "valid_from": valid_from, "valid_to": None if is_current else timeline[i + 1][1],
            "is_current": is_current
        })
    return history


def generate_orders_data(users_table, product_catalog, product_pricing, product_inventory, user_location_table, location_table, user_status_history_table):
    orders_table, order_details_table, order_status_history_table = [], [], []
    order_items_table, order_item_pricing_table = [], []
    order_payment_info = []
    order_purchase_meta = []

    user_locs_map = build_user_state_lookup(user_location_table, location_table)
    user_status_map = build_user_status_lookup(user_status_history_table)
    weight_by_product = {row["product_id"]: row["weight_g"] for row in product_inventory}
    
    product_catalog_map = build_product_catalog_lookup(product_catalog)

    global_item_counter, global_status_history_counter = 1, 1
    user_ids = [u["user_id"] for u in users_table]
    all_product_ids = list(product_catalog_map.keys())

    for i in range(NUMBER_OF_ORDERS):
        order_id = f"ORDER-{str(i + 1).zfill(10)}"
        
        # Pick an active/eligible user at order creation time
        for _ in range(50):
            user_id = random.choice(user_ids)
            created_at = random_date(FOUNDATION_END, NOW - timedelta(days=1))
            if is_user_eligible_to_order_at_date(user_id, created_at, user_status_map):
                break

        shipping_method = random.choice(["STANDARD", "EXPRESS"])
        payment_started_at = created_at + timedelta(minutes=random.randint(1, 15))
        paid_at = payment_started_at + timedelta(minutes=random.randint(1, 5))

        status_history = generate_order_status_history(created_at, paid_at, shipping_method)
        for status_record in status_history:
            order_status_history_table.append({
                "id": str(global_status_history_counter).zfill(10), "order_id": order_id, "status": status_record["status"],
                "status_change_description": status_record["status_change_description"], "valid_from": status_record["valid_from"],
                "valid_to": status_record["valid_to"], "is_current": status_record["is_current"],
            })
            global_status_history_counter += 1

        # Filter products active at exact order timestamp
        valid_product_ids = [
            p_id for p_id in all_product_ids
            if is_product_active_at_date_fast(p_id, created_at, product_catalog_map)
        ]
        if not valid_product_ids:
            valid_product_ids = all_product_ids

        num_items = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
        selected_products = random.sample(valid_product_ids, min(num_items, len(valid_product_ids)))

        items_value = 0.0
        total_weight_g = 0
        order_items_data_temp = []

        for seq, product_id in enumerate(selected_products, start=1):
            order_item_id = f"ORDER-ITEM-{str(global_item_counter).zfill(12)}"
            global_item_counter += 1

            unit_price = get_historical_price(product_id, created_at, product_pricing)
            quantity = random.randint(1, 3)
            discount = round(unit_price * random.choice([0.0, 0.0, 0.05, 0.10]), 2)

            items_value += round((unit_price - discount) * quantity, 2)
            total_weight_g += weight_by_product[product_id] * quantity

            order_items_table.append({"order_item_id": order_item_id, "order_id": order_id, "order_item_seq": seq, "product_id": product_id, "quantity": quantity})
            order_item_pricing_table.append({"order_item_id": order_item_id, "unit_price": unit_price, "discount": discount})
            
            order_items_data_temp.append({"product_id": product_id, "quantity": quantity})

        state = get_user_state_at_date(user_id, created_at, user_locs_map)
        freight_value = calculate_freight_value(state, shipping_method, total_weight_g)

        order_details_table.append({"order_id": order_id, "order_channel": random.choice(["WEB", "MOBILE_APP", "MARKETPLACE"]), "shipping_method": shipping_method, "freight_value": freight_value})

        order_payment_info.append({
            "order_id": order_id, "payment_started_at": payment_started_at, "paid_at": paid_at,
            "total_value": round(items_value + freight_value, 2),
        })

        order_purchase_meta.append({
            "order_id": order_id,
            "user_id": user_id,
            "created_at": created_at,
            "items": order_items_data_temp
        })

    return orders_table, order_details_table, order_status_history_table, order_items_table, order_item_pricing_table, order_payment_info, order_purchase_meta

# =============================================================================
# SESSIONS & CLICKSTREAM MODULE (DIRECT FOREIGN KEY LINK)
# =============================================================================
def generate_sessions_and_clickstreams(users_table, product_catalog, order_purchase_meta):
    raw_sessions = []

    device_types = ["Desktop", "Mobile", "Tablet"]
    browsers = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
    traffic_sources = ["Google", "Direct", "Facebook", "Newsletter", "Twitter"]

    user_ids = [u["user_id"] for u in users_table]
    product_ids = [p["product_id"] for p in product_catalog]

    # 1. Prepare Purchase Sessions (linked directly to orders)
    for order in order_purchase_meta:
        order_id = order["order_id"]
        user_id = order["user_id"]
        created_at = order["created_at"]
        items = order["items"]

        session_duration = random.randint(5, 30)
        session_start = created_at - timedelta(minutes=session_duration)
        session_end = created_at

        raw_sessions.append({
            "type": "purchase",
            "order_id": order_id,
            "user_id": user_id,
            "session_start": session_start,
            "session_end": session_end,
            "device_type": random.choice(device_types),
            "browser": random.choice(browsers),
            "traffic_source": random.choice(traffic_sources),
            "items": items
        })

    # 2. Prepare General Non-Purchase Sessions
    for _ in range(NUMBER_OF_GENERAL_SESSIONS):
        user_id = random.choice(user_ids)
        session_start = random_date(FOUNDATION_END, NOW - timedelta(hours=1))
        session_duration = random.randint(1, 25)
        session_end = session_start + timedelta(minutes=session_duration)

        raw_sessions.append({
            "type": "general",
            "order_id": None,
            "user_id": user_id,
            "session_start": session_start,
            "session_end": session_end,
            "device_type": random.choice(device_types),
            "browser": random.choice(browsers),
            "traffic_source": random.choice(traffic_sources),
            "items": []
        })

    # 3. Sort all sessions chronologically for seamless blending
    raw_sessions.sort(key=lambda x: x["session_start"])

    orders_table = []
    sessions_table = []
    clickstream_table = []
    
    session_counter = 1
    event_counter = 1

    for raw in raw_sessions:
        session_id = f"SES-{str(session_counter).zfill(10)}"
        session_counter += 1

        sessions_table.append({
            "session_id": session_id,
            "user_id": raw["user_id"],
            "session_start": raw["session_start"],
            "session_end": raw["session_end"],
            "device_type": raw["device_type"],
            "browser": raw["browser"],
            "traffic_source": raw["traffic_source"]
        })

        if raw["type"] == "purchase":
            # Add session_id directly to orders table
            orders_table.append({
                "order_id": raw["order_id"],
                "user_id": raw["user_id"],
                "session_id": session_id,
                "created_at": raw["session_end"]
            })

            current_time = raw["session_start"]
            clickstream_table.append({
                "session_id": session_id, 
                "user_id": raw["user_id"], 
                "event_id": f"EVT-{str(event_counter).zfill(12)}",
                "product_id": "", 
                "event_time": current_time, 
                "event_type": "page_view"
            })
            event_counter += 1
            current_time += timedelta(seconds=random.randint(5, 20))

            for item in raw["items"]:
                p_id = item["product_id"]
                qty = item["quantity"]

                clickstream_table.append({
                    "session_id": session_id, 
                    "user_id": raw["user_id"], 
                    "event_id": f"EVT-{str(event_counter).zfill(12)}",
                    "product_id": p_id, 
                    "event_time": current_time, 
                    "event_type": "view_product"
                })
                event_counter += 1
                current_time += timedelta(seconds=random.randint(5, 15))

                for _ in range(qty):
                    clickstream_table.append({
                        "session_id": session_id, 
                        "user_id": raw["user_id"], 
                        "event_id": f"EVT-{str(event_counter).zfill(12)}",
                        "product_id": p_id, 
                        "event_time": current_time, 
                        "event_type": "add_to_cart"
                    })
                    event_counter += 1
                    current_time += timedelta(seconds=random.randint(2, 8))

            clickstream_table.append({
                "session_id": session_id, 
                "user_id": raw["user_id"], 
                "event_id": f"EVT-{str(event_counter).zfill(12)}",
                "product_id": "", 
                "event_time": raw["session_end"], 
                "event_type": "checkout"
            })
            event_counter += 1

        else:
            num_events = random.randint(2, 6)
            curr_time = raw["session_start"]
            for _ in range(num_events):
                if curr_time >= raw["session_end"]:
                    break
                ev_type = random.choice(["page_view", "search", "view_product", "remove_from_cart"])
                p_id = random.choice(product_ids) if ev_type in ["view_product", "remove_from_cart"] else ""
                
                clickstream_table.append({
                    "session_id": session_id,
                    "user_id": raw["user_id"],
                    "event_id": f"EVT-{str(event_counter).zfill(12)}",
                    "product_id": p_id,
                    "event_time": curr_time,
                    "event_type": ev_type
                })
                event_counter += 1
                curr_time += timedelta(seconds=random.randint(10, 180))

    return orders_table, sessions_table, clickstream_table


# =============================================================================
# MARCIN MODULE: PAYMENTS
# =============================================================================
def generate_payments_domain_tables(order_payment_info):
    details, history = [], []
    payment_types = ["CREDIT_CARD", "BLIK", "VOUCHER", "BANK_TRANSFER"]

    for order in order_payment_info:
        order_id = order["order_id"]
        total_value = order["total_value"]

        payment_date = order["payment_started_at"]
        end_time = order["paid_at"]

        num_payments = 1 if random.random() < 0.8 else 2
        values = []
        if num_payments == 1:
            values = [total_value]
        else:
            first_val = round(total_value * random.uniform(0.1, 0.5), 2)
            values = [first_val, round(total_value - first_val, 2)]

        order_payment_types = random.sample(payment_types, num_payments)

        for seq in range(1, num_payments + 1):
            pay_id = f"PAY-{len(details) + 1}"
            p_type = order_payment_types[seq - 1]
            val = values[seq - 1]
            installments = random.choice([1, 3, 6, 12]) if p_type == "CREDIT_CARD" else 1

            details.append({"payment_id": pay_id, "order_id": order_id, "payment_sequential": seq, "payment_type": p_type, "payment_installments": installments, "payment_value": val})

            status_final = "COMPLETED"

            history.append({"log_id": f"LOG-{len(history) + 1}", "payment_id": pay_id, "status": "PENDING", "gateway_response": "100_INIT", "valid_from": payment_date, "valid_to": end_time, "is_current": 0})
            history.append({"log_id": f"LOG-{len(history) + 1}", "payment_id": pay_id, "status": status_final, "gateway_response": "200_OK", "valid_from": end_time, "valid_to": None, "is_current": 1})

    return details, history


# =============================================================================
# UNIFIED ERROR INJECTION
# =============================================================================
def inject_incorrect_data(
    orders_table, order_items_table, order_item_pricing_table, order_status_history_table,
    categories_table, product_catalog_table, product_inventory_table, product_pricing_history_table,
    payment_details_table, payment_status_history_table, user_contact_table
):
    for row in user_contact_table:
        if random.random() < INVALID_EMAILS:
            mutations = [lambda e: f" {e}", lambda e: e.replace(" ", ""), lambda e: e.replace("@", "@@", 1), lambda e: e.split("@")[0], lambda e: e.replace("@", "")]
            row["email"] = random.choice(mutations)(row["email"])
        if random.random() < INVALID_PHONE_NUMBERS:
            mutations = [lambda p: f" {p}", lambda p: p.replace("-", ""), lambda p: "123", lambda p: "not-a-phone"]
            row["phone"] = random.choice(mutations)(row["phone"])

    for row in orders_table:
        if random.random() < NULL_RATE: row["user_id"] = None
        if random.random() < WRONG_DATE_RATE: row["created_at"] = datetime(1900, 1, 1, 0, 0, 0) if random.choice([True, False]) else datetime(2099, 12, 31, 23, 59, 59)

    for row in order_items_table:
        if random.random() < NULL_RATE: row["product_id"] = None

    for row in order_item_pricing_table:
        if random.random() < WRONG_PRICE_RATE: row["unit_price"] = -abs(row["unit_price"])
        if random.random() < WRONG_PRICE_RATE: row["discount"] = -abs(row["discount"])

    for i in range(len(order_status_history_table) - 1):
        if order_status_history_table[i]["order_id"] == order_status_history_table[i + 1]["order_id"]:
            if random.random() < WRONG_DATE_RATE:
                order_status_history_table[i]["valid_from"], order_status_history_table[i + 1]["valid_from"] = order_status_history_table[i + 1]["valid_from"], order_status_history_table[i]["valid_from"]

    if len(categories_table) > 5 and random.random() < CATEGORY_ORPHAN_RATE: categories_table[5]["parent_category_id"] = "CAT-99999"

    for row in product_catalog_table:
        if random.random() < PRODUCT_MISSING_CAT_RATE: row["category_id"] = None
        if row["is_active"] == 1 and random.random() < PRODUCT_MISSING_NAME_RATE: row["product_name"] = ""

    for row in product_inventory_table:
        if random.random() < INVENTORY_NEGATIVE_STOCK_RATE: row["stock_quantity"] = -abs(row["stock_quantity"])
        if random.random() < INVENTORY_NEGATIVE_DIMENSION_RATE: row["weight_g"] = -abs(row["weight_g"])

    for row in product_pricing_history_table:
        if random.random() < PRICING_NEGATIVE_PRICE_RATE: row["price"] = -abs(row["price"])
        if row["is_current"] == 0 and random.random() < PRICING_SCD2_GAP_RATE and row["valid_to"] is not None:
            row["valid_to"] = row["valid_to"] - timedelta(days=2)

    for row in payment_details_table:
        if random.random() < PAYMENT_NEGATIVE_VALUE_RATE: row["payment_value"] = -abs(row["payment_value"])
        if random.random() < PAYMENT_INVALID_TYPE_RATE: row["payment_type"] = "UNKNOWN"
        if random.random() < PAYMENT_INVALID_INSTALLMENTS_RATE and row["payment_type"] == "VOUCHER": row["payment_installments"] = 12
        if random.random() < PAYMENT_DUPLICATE_SEQ_RATE and row["payment_sequential"] == 2: row["payment_sequential"] = 1

    for i in range(len(payment_status_history_table) - 1):
        curr_log = payment_status_history_table[i]
        next_log = payment_status_history_table[i + 1]
        if curr_log["payment_id"] == next_log["payment_id"]:
            if random.random() < PAYMENT_STATUS_ANOMALY_RATE: curr_log["is_current"] = 1
            if random.random() < PAYMENT_STATUS_ANOMALY_RATE:
                curr_log["valid_from"], next_log["valid_from"] = next_log["valid_from"], curr_log["valid_from"]


# -----------------------------------------------------------------------------
# EXPORT UTILITIES & MAIN RUNNER
# -----------------------------------------------------------------------------
def format_csv_value(value):
    if isinstance(value, datetime): return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date): return value.strftime("%Y-%m-%d")
    return value


def write_csv_file(destination, data):
    if not data: return
    field_names = data[0].keys()
    with open(destination, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        for row in data:
            writer.writerow({key: format_csv_value(value) for key, value in row.items()})


def write_json_file(destination, data):
    if not data: return
    serialized_data = []
    for row in data:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, (datetime, date)):
                new_row[k] = v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v.strftime("%Y-%m-%d")
            else:
                new_row[k] = v
        serialized_data.append(new_row)
        
    with open(destination, "w", encoding="utf-8") as file:
        json.dump(serialized_data, file, indent=4, ensure_ascii=False)


def write_parquet_file(destination, data):
    if not data: return
    df = pd.DataFrame(data)
    df.to_parquet(destination, index=False, engine='fastparquet')


def main():
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    
    # 1. Generate Foundation (Users, Categories, Products)
    users, profiles, contacts, locs_hist, locs, status_hist = generate_data()
    categories, cat_mapping = generate_categories_table()
    prod_cat, prod_inv, prod_price = generate_products_domain_tables(NUMBER_OF_PRODUCTS, cat_mapping)

    # 2. Generate Orders & Order Metadata (passing status_hist for user eligibility validation)
    _, ord_details, ord_history, ord_items, ord_item_price, order_payment_info, order_purchase_meta = generate_orders_data(
        users, prod_cat, prod_price, prod_inv, locs_hist, locs, status_hist
    )

    # 3. Generate Sessions, Clickstream & Orders Table with session_id linked directly
    ord_table, sessions, clickstream = generate_sessions_and_clickstreams(users, prod_cat, order_purchase_meta)

    # 4. Generate Payments
    pay_details, pay_history = generate_payments_domain_tables(order_payment_info)

    # 5. Inject Errors
    #inject_incorrect_data(
    #    ord_table, ord_items, ord_item_price, ord_history,
    #    categories, prod_cat, prod_inv, prod_price,
    #    pay_details, pay_history, contacts
    #)

    # 6. Multi-Format Export Mapping
    json_tables = {
        "categories_table.json": categories,
        "product_catalog_table.json": prod_cat,
        "product_inventory_table.json": prod_inv,
        "product_pricing_history_table.json": prod_price,
    }

    parquet_tables = {
        "orders_table.parquet": ord_table,
        "order_details_table.parquet": ord_details,
        "order_status_history_table.parquet": ord_history,
        "order_items_table.parquet": ord_items,
        "order_item_pricing_table.parquet": ord_item_price,
        "payment_details_table.parquet": pay_details,
        "payment_status_history_table.parquet": pay_history,
        "session.parquet": sessions,
        "event_clickstream.parquet": clickstream,
    }

    csv_tables = {
        "users_table.csv": users,
        "user_profile_table.csv": profiles,
        "user_contact_table.csv": contacts,
        "user_location_table.csv": locs_hist,
        "location_table.csv": locs,
        "user_status_history_table.csv": status_hist,
    }

    print("--- GENERATING MULTI-FORMAT DATA FILES ---")
    
    for filename, rows in json_tables.items():
        write_json_file(os.path.join(OUTPUT_DIRECTORY, filename), rows)
        print(f"[JSON] [{filename}]: {len(rows)} rows generated.")

    for filename, rows in parquet_tables.items():
        write_parquet_file(os.path.join(OUTPUT_DIRECTORY, filename), rows)
        print(f"[PARQUET] [{filename}]: {len(rows)} rows generated.")

    for filename, rows in csv_tables.items():
        write_csv_file(os.path.join(OUTPUT_DIRECTORY, filename), rows)
        print(f"[CSV] [{filename}]: {len(rows)} rows generated.")

if __name__ == "__main__":
    main()