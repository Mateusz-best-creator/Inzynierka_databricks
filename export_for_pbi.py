import os
import duckdb

con = duckdb.connect()
gold_dir = os.path.abspath("./local_warehouse/ecommerce_gold.db")
export_dir = os.path.abspath("./powerbi_data")
os.makedirs(export_dir, exist_ok=True)

# Export each Gold Delta table into a single flat Parquet file for Power BI
for table_name in os.listdir(gold_dir):
    table_path = os.path.join(gold_dir, table_name).replace("\\", "/")
    if os.path.isdir(table_path):
        out_file = os.path.join(export_dir, f"{table_name}.parquet").replace("\\", "/")
        con.sql(
            f"COPY (SELECT * FROM delta_scan('{table_path}')) TO '{out_file}' (FORMAT PARQUET)"
        )
        print(f"Exported {table_name}.parquet")
