"""Generates synthetic product category, catalog, inventory, and pricing tables.

Builds a two-level category hierarchy and generates SCD Type 2 history records
for product catalog metadata, physical dimensions and stock levels, and
historical pricing/cost margins.
"""

from datetime import datetime
import random

import config
import utils

HEAVY_SUBCATEGORIES = ("Furniture", "Fitness", "Cycling")
MEDIUM_SUBCATEGORIES = ("Laptops", "Kitchen", "Cameras", "Lighting")
LIGHT_SUBCATEGORIES = (
    "Smartphones",
    "Audio",
    "Decor",
    "Running",
    "Swimming",
    "Men",
    "Women",
    "Kids",
    "Accessories",
)

STOCK_WEIGHTS = [8, 22, 50, 20]
PRICE_TIER_WEIGHTS = [40, 35, 18, 7]
MARGIN_WEIGHTS = [20, 50, 30]


def generate_categories_table() -> tuple[list[dict], dict[str, str]]:
    """Generates the two-level product category hierarchy table.

    Returns:
        Tuple of (categories, cat_mapping), where categories is the list of
        category records and cat_mapping maps subcategory IDs to names.
    """
    categories: list[dict] = []
    cat_counter = 1
    cat_mapping: dict[str, str] = {}

    for main_cat, sub_cats in config.CATEGORY_STRUCTURE.items():
        main_id = f"CAT-{str(cat_counter).zfill(5)}"
        categories.append(
            {
                "category_id": main_id,
                "parent_category_id": None,
                "category_name": main_cat,
                "created_at": utils.random_date(
                    config.FOUNDATION_START, config.FOUNDATION_END
                ),
            }
        )
        cat_counter += 1

        for sub_cat in sub_cats:
            sub_id = f"CAT-{str(cat_counter).zfill(5)}"
            cat_mapping[sub_id] = sub_cat
            categories.append(
                {
                    "category_id": sub_id,
                    "parent_category_id": main_id,
                    "category_name": sub_cat,
                    "created_at": utils.random_date(
                        config.FOUNDATION_START, config.FOUNDATION_END
                    ),
                }
            )
            cat_counter += 1

    return categories, cat_mapping


def generate_physical_specs(subcat_name: str) -> tuple[int, int, int, int]:
    """Generates realistic weight and package dimensions for a subcategory.

    Args:
        subcat_name: Product subcategory name.

    Returns:
        Tuple of (weight_g, length_cm, height_cm, width_cm).
    """
    if subcat_name in HEAVY_SUBCATEGORIES:
        weight = random.randint(5500, 28000)
        length = random.randint(40, 120)
        width = random.randint(30, 80)
        height = random.randint(25, 80)
    elif subcat_name in MEDIUM_SUBCATEGORIES:
        weight = random.randint(600, 4800)
        length = random.randint(15, 45)
        width = random.randint(12, 35)
        height = random.randint(8, 25)
    elif subcat_name in LIGHT_SUBCATEGORIES:
        weight = random.randint(80, 900)
        length = random.randint(5, 25)
        width = random.randint(5, 20)
        height = random.randint(2, 12)
    return weight, length, height, width


def append_catalog_versions(
    catalog: list[dict],
    prod_id: str,
    cat_id: str,
    product_base_name: str,
    brand: str,
    created_at: datetime,
) -> tuple[bool, datetime | None]:
    """Appends SCD Type 2 catalog versions for a single product.

    Args:
        catalog: List collecting product catalog records.
        prod_id: Product identifier.
        cat_id: Leaf category identifier.
        product_base_name: Generated product display name.
        brand: Generated brand name.
        created_at: Initial product creation timestamp.

    Returns:
        Tuple of (discontinued, discontinuation_date).
    """
    cat_dates = utils.generate_scd2_timeline(created_at, random.randint(1, 3))
    num_cat_versions = len(cat_dates)
    discontinued = (
        random.random() < config.DISCONTINUATION_RATE and num_cat_versions > 1
    )
    discontinuation_date: datetime | None = None

    for j in range(num_cat_versions):
        is_current = 1 if j == num_cat_versions - 1 else 0

        if discontinued and j == num_cat_versions - 1:
            is_active = 0
            description = "Product has been discontinued."
            valid_to = None
            discontinuation_date = cat_dates[j]
        else:
            is_active = 1
            description = (
                random.choice(utils.POOL_DESCRIPTIONS)
                if j == 0
                else "Product details updated."
            )
            valid_to = None if is_current else cat_dates[j + 1]

        catalog.append(
            {
                "id": str(len(catalog) + 1).zfill(10),
                "product_id": prod_id,
                "category_id": cat_id,
                "product_name": product_base_name,
                "brand": brand,
                "description": description,
                "is_active": is_active,
                "valid_from": cat_dates[j],
                "valid_to": valid_to,
                "is_current": is_current,
            }
        )

    return discontinued, discontinuation_date


def append_inventory_versions(
    inventory: list[dict],
    prod_id: str,
    subcat_name: str,
    created_at: datetime,
    discontinued: bool,
    discontinuation_date: datetime | None,
) -> None:
    """Appends SCD Type 2 inventory and physical dimension versions for a product.

    Args:
        inventory: List collecting product inventory records.
        prod_id: Product identifier.
        subcat_name: Product subcategory name.
        created_at: Initial product creation timestamp.
        discontinued: Whether the product was eventually discontinued.
        discontinuation_date: Timestamp when the product became inactive, if any.
    """
    inv_dates = utils.generate_scd2_timeline(created_at, random.randint(1, 4))
    if discontinuation_date:
        inv_dates = [d for d in inv_dates if d <= discontinuation_date]
        if not inv_dates:
            inv_dates = [created_at]

    weight, length, height, width = generate_physical_specs(subcat_name)

    for j in range(len(inv_dates)):
        is_last = j == len(inv_dates) - 1
        is_current = 1 if is_last else 0
        valid_to = None if is_last else inv_dates[j + 1]

        if is_last and discontinued:
            stock_qty = 0
        else:
            stock_qty = random.choices(
                [
                    0,
                    random.randint(1, 10),
                    random.randint(11, 100),
                    random.randint(101, 250),
                ],
                weights=STOCK_WEIGHTS,
            )[0]

        inventory.append(
            {
                "inventory_id": f"INV-{len(inventory) + 1}",
                "product_id": prod_id,
                "weight_g": weight,
                "length_cm": length,
                "height_cm": height,
                "width_cm": width,
                "stock_quantity": stock_qty,
                "valid_from": inv_dates[j],
                "valid_to": valid_to,
                "is_current": is_current,
            }
        )


def append_pricing_versions(
    pricing: list[dict],
    prod_id: str,
    created_at: datetime,
    discontinuation_date: datetime | None,
) -> None:
    """Appends SCD Type 2 retail price and cost price history versions for a product.

    Args:
        pricing: List collecting product pricing history records.
        prod_id: Product identifier.
        created_at: Initial product creation timestamp.
        discontinuation_date: Timestamp when the product became inactive, if any.
    """
    pricing_dates = utils.generate_scd2_timeline(created_at, random.randint(1, 4))
    if discontinuation_date:
        pricing_dates = [d for d in pricing_dates if d <= discontinuation_date]
        if not pricing_dates:
            pricing_dates = [created_at]

    base_price_tier = random.choices(
        [
            random.uniform(10.0, 30.0),
            random.uniform(30.0, 100.0),
            random.uniform(100.0, 300.0),
            random.uniform(300.0, 800.0),
        ],
        weights=PRICE_TIER_WEIGHTS,
        k=1,
    )[0]
    current_price = round(base_price_tier, 2)

    for j in range(len(pricing_dates)):
        is_last = j == len(pricing_dates) - 1
        is_current = 1 if is_last else 0
        valid_to = None if is_last else pricing_dates[j + 1]

        if j > 0:
            current_price = round(
                max(5.0, current_price * random.uniform(0.85, 1.25)), 2
            )

        margin_ratio = random.choices(
            [
                random.uniform(0.88, 0.95),
                random.uniform(0.68, 0.82),
                random.uniform(0.45, 0.62),
            ],
            weights=MARGIN_WEIGHTS,
        )[0]

        pricing.append(
            {
                "unique_id": f"PRC-{len(pricing) + 1}",
                "product_id": prod_id,
                "price": current_price,
                "cost_price": round(current_price * margin_ratio, 2),
                "valid_from": pricing_dates[j],
                "valid_to": valid_to,
                "is_current": is_current,
            }
        )


def generate_products_domain_tables(
    number_of_products: int, cat_mapping: dict[str, str]
) -> tuple[list[dict], list[dict], list[dict]]:
    """Generates product catalog, inventory, and pricing history tables.

    Args:
        number_of_products: Total number of distinct products to generate.
        cat_mapping: Mapping of subcategory IDs to subcategory names.

    Returns:
        Tuple of (catalog, inventory, pricing) record lists.
    """
    catalog: list[dict] = []
    inventory: list[dict] = []
    pricing: list[dict] = []
    subcat_ids = list(cat_mapping.keys())

    for i in range(1, number_of_products + 1):
        prod_id = f"PRD-{str(i).zfill(10)}"
        cat_id = random.choice(subcat_ids)
        subcat_name = cat_mapping[cat_id]
        created_at = utils.random_date(config.FOUNDATION_START, config.FOUNDATION_END)

        product_base_name = (
            f"Item {random.choice(config.CATEGORY_NOUNS[subcat_name])} Pro"
        )
        brand = random.choice(utils.POOL_COMPANIES)

        discontinued, discontinuation_date = append_catalog_versions(
            catalog, prod_id, cat_id, product_base_name, brand, created_at
        )
        append_inventory_versions(
            inventory,
            prod_id,
            subcat_name,
            created_at,
            discontinued,
            discontinuation_date,
        )
        append_pricing_versions(pricing, prod_id, created_at, discontinuation_date)

    return catalog, inventory, pricing
