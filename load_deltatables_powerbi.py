from deltalake import DeltaTable

# Base path to your ecommerce_gold.db directory
base_path = "C:/Studies/Inzynierka_databricks/local_warehouse/ecommerce_gold.db/"

# Fact Table
fact_events = DeltaTable(base_path + "fact_events").to_pandas()
fact_sales = DeltaTable(base_path + "fact_sales").to_pandas()

# Dimension Tables
dim_user = DeltaTable(base_path + "dim_user").to_pandas()
dim_product = DeltaTable(base_path + "dim_product").to_pandas()
dim_date = DeltaTable(base_path + "dim_date").to_pandas()
dim_event_type = DeltaTable(base_path + "dim_event_type").to_pandas()
dim_geography = DeltaTable(base_path + "dim_geography").to_pandas()
dim_payment_type = DeltaTable(base_path + "dim_payment_type").to_pandas()
dim_sales_context = DeltaTable(base_path + "dim_sales_context").to_pandas()
dim_session = DeltaTable(base_path + "dim_session").to_pandas()
dim_time = DeltaTable(base_path + "dim_time").to_pandas()
