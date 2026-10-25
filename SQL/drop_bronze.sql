use InzynierkaDB;

DROP TABLE IF EXISTS bronze_payment_status_history_table;
DROP TABLE IF EXISTS bronze_payment_details_table;
DROP TABLE IF EXISTS bronze_order_item_pricing_table;
DROP TABLE IF EXISTS bronze_order_items_table;
DROP TABLE IF EXISTS bronze_order_status_history_table;
DROP TABLE IF EXISTS bronze_order_details_table;
DROP TABLE IF EXISTS bronze_orders_table;

DROP TABLE IF EXISTS bronze_event_clickstream;
DROP TABLE IF EXISTS bronze_session;

DROP TABLE IF EXISTS bronze_product_pricing_history_table;
DROP TABLE IF EXISTS bronze_product_inventory_table;
DROP TABLE IF EXISTS bronze_product_catalog_table;
DROP TABLE IF EXISTS bronze_categories_table;

DROP TABLE IF EXISTS bronze_user_status_history_table;
DROP TABLE IF EXISTS bronze_user_location_table;
DROP TABLE IF EXISTS bronze_user_contact_table;
DROP TABLE IF EXISTS bronze_user_profile_table;
DROP TABLE IF EXISTS bronze_users_table;
DROP TABLE IF EXISTS bronze_location_table;