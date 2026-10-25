import os
import pandas as pd
from sqlalchemy import create_engine

SERVER = "."  
DATABASE = "InzynierkaDB"

engine = create_engine(
    f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

DATA_DIR = "generated_combined_data"  

ordered_load_steps = [
    ("categories_table.json", "bronze_categories_table"),
    ("location_table.csv", "bronze_location_table"),
    ("users_table.csv", "bronze_users_table"),
    
    ("user_profile_table.csv", "bronze_user_profile_table"),
    ("user_contact_table.csv", "bronze_user_contact_table"),
    ("user_location_table.csv", "bronze_user_location_table"),
    ("user_status_history_table.csv", "bronze_user_status_history_table"),
    
    ("product_catalog_table.json", "bronze_product_catalog_table"),
    ("product_inventory_table.json", "bronze_product_inventory_table"),
    ("product_pricing_history_table.json", "bronze_product_pricing_history_table"),
    
    ("session.parquet", "bronze_session"),
    
    ("orders_table.parquet", "bronze_orders_table"),
    ("order_details_table.parquet", "bronze_order_details_table"),
    ("order_status_history_table.parquet", "bronze_order_status_history_table"),
    
    ("order_items_table.parquet", "bronze_order_items_table"),
    ("order_item_pricing_table.parquet", "bronze_order_item_pricing_table"),
    
    ("payment_details_table.parquet", "bronze_payment_details_table"),
    ("payment_status_history_table.parquet", "bronze_payment_status_history_table"),
    
    ("event_clickstream.parquet", "bronze_event_clickstream")
]

id_cols_to_enforce = [
    'user_id', 'session_id', 'order_id', 'product_id', 'category_id', 
    'payment_id', 'order_item_id', 'location_id', 'event_id', 'id', 
    'unique_id', 'log_id', 'inventory_id'
]

for filename, table_name in ordered_load_steps:
    file_path = os.path.join(DATA_DIR, filename)
    
    if not os.path.exists(file_path):
        print(f"Warning: File {filename} not found in {DATA_DIR}. Skipping.")
        continue

    try:
        if filename.endswith(".csv"):
            col_types = {col: str for col in id_cols_to_enforce}
            df = pd.read_csv(file_path, dtype=col_types, low_memory=False)
        elif filename.endswith(".json"):
            df = pd.read_json(file_path)
        elif filename.endswith(".parquet"):
            df = pd.read_parquet(file_path)
        else:
            continue

        for col in id_cols_to_enforce:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)
                if col == 'user_id':
                    df[col] = df[col].str.zfill(10)

        df.to_sql(
            table_name, con=engine, if_exists="append", index=False, chunksize=1000
        )
        print(f"Successfully loaded {filename} into [{table_name}]")
        
    except Exception as e:
        print(f"Error loading {filename} into {table_name}: {e}")
        break  # Stop execution if a dependency fails