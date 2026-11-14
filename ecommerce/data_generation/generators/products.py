import random
import config
import utils


def generate_categories_table():
    categories = []
    cat_counter = 1
    cat_mapping = {}

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


def generate_physical_specs(subcat_name: str):
    if subcat_name in ("Furniture", "Fitness", "Cycling"):
        weight = random.randint(5500, 28000)
        length = random.randint(40, 120)
        width = random.randint(30, 80)
        height = random.randint(25, 80)
    elif subcat_name in ("Laptops", "Kitchen", "Cameras", "Lighting"):
        weight = random.randint(600, 4800)
        length = random.randint(15, 45)
        width = random.randint(12, 35)
        height = random.randint(8, 25)
    else:
        weight = random.randint(80, 900)
        length = random.randint(5, 25)
        width = random.randint(5, 20)
        height = random.randint(2, 12)
    return weight, length, height, width


def generate_products_domain_tables(number_of_products, cat_mapping):
    catalog, inventory, pricing = [], [], []
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

        cat_dates = utils.generate_scd2_timeline(created_at, random.randint(1, 3))
        num_cat_versions = len(cat_dates)
        discontinued = (
            random.random() < config.DISCONTINUATION_RATE and num_cat_versions > 1
        )
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
                    weights=[8, 22, 50, 20],
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
            weights=[40, 35, 18, 7],
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
                weights=[20, 50, 30],
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

    return catalog, inventory, pricing
