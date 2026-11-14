import os
import config
from export import write_csv_file, write_json_file, write_parquet_file
from generators.users import generate_data
from generators.products import (
    generate_categories_table,
    generate_products_domain_tables,
)
from generators.orders import generate_orders_data
from generators.behavioral import generate_sessions_and_clickstreams
from generators.payments import generate_payments_domain_tables
from generators.errors import inject_incorrect_data


def main():
    os.makedirs(config.OUTPUT_DIRECTORY, exist_ok=True)

    # 1. Foundation Domains
    users, profiles, contacts, locs_hist, locs, status_hist = generate_data()
    categories, cat_mapping = generate_categories_table()
    prod_cat, prod_inv, prod_price = generate_products_domain_tables(
        config.NUMBER_OF_PRODUCTS, cat_mapping
    )

    # 2. Orders & Purchase Metadata
    (
        _,
        ord_details,
        ord_history,
        ord_items,
        ord_item_price,
        order_payment_info,
        order_purchase_meta,
    ) = generate_orders_data(
        users, prod_cat, prod_price, prod_inv, locs_hist, locs, status_hist
    )

    # 3. Web Sessions & Clickstreams
    ord_table, sessions, clickstream = generate_sessions_and_clickstreams(
        users, prod_cat, order_purchase_meta
    )

    # 4. Payments & Audit Logs
    pay_details, pay_history = generate_payments_domain_tables(order_payment_info)

    # 5. Error Injection Toggle
    if config.INJECT_ERRORS:
        print("[INFO] Injecting synthetic error patterns into output datasets...")
        inject_incorrect_data(
            ord_table,
            ord_items,
            ord_item_price,
            ord_history,
            categories,
            prod_cat,
            prod_inv,
            prod_price,
            pay_details,
            pay_history,
            contacts,
        )

    # 6. Multi-Format Exports
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
        write_json_file(os.path.join(config.OUTPUT_DIRECTORY, filename), rows)
        print(f"[JSON] [{filename}]: {len(rows)} rows generated.")

    for filename, rows in parquet_tables.items():
        write_parquet_file(os.path.join(config.OUTPUT_DIRECTORY, filename), rows)
        print(f"[PARQUET] [{filename}]: {len(rows)} rows generated.")

    for filename, rows in csv_tables.items():
        write_csv_file(os.path.join(config.OUTPUT_DIRECTORY, filename), rows)
        print(f"[CSV] [{filename}]: {len(rows)} rows generated.")


if __name__ == "__main__":
    main()
