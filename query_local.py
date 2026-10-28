import os
import duckdb

# Initialize in-memory DuckDB connection
con = duckdb.connect()

gold_dir = os.path.abspath("./local_warehouse/ecommerce_gold.db")

# Register all Gold Delta tables as SQL views
for table_name in os.listdir(gold_dir):
    table_path = os.path.join(gold_dir, table_name).replace("\\", "/")
    if os.path.isdir(table_path):
        con.sql(f"CREATE VIEW {table_name} AS SELECT * FROM delta_scan('{table_path}')")

print("Gold tables registered! Running sample SQL queries:\n")

# Example 1: Check row counts across all Gold tables
counts_query = """
SELECT 'dim_user' AS table_name, COUNT(*) AS rows FROM dim_user
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_session', COUNT(*) FROM dim_session
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
UNION ALL SELECT 'fact_events', COUNT(*) FROM fact_events
"""
con.sql(counts_query).show()

# Example 2: Analytical SQL query (Top 5 categories by net revenue)
revenue_query = """
SELECT
    p.CATEGORY_NAME,
    p.PRICE_TIER,
    COUNT(DISTINCT fs.ORDER_ID) AS total_orders,
    ROUND(SUM(fs.NET_ITEM_AMOUNT), 2) AS net_revenue,
    ROUND(AVG(fs.NET_PROFIT_MARGIN) * 100, 2) AS avg_margin_pct
FROM fact_sales fs
JOIN dim_product p ON fs.PRODUCT_SURROGATE_KEY = p.PRODUCT_SURROGATE_KEY
GROUP BY p.CATEGORY_NAME, p.PRICE_TIER
ORDER BY net_revenue DESC
LIMIT 5
"""
con.sql(revenue_query).show()
