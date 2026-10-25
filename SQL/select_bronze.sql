USE InzynierkaDB; 
GO

-- USERS & LOCATIONS
SELECT * FROM bronze_location_table;

SELECT TOP 10 * FROM bronze_users_table;

SELECT TOP 10 * FROM bronze_user_profile_table;

SELECT TOP 10 * FROM bronze_user_contact_table;

SELECT TOP 10 * FROM bronze_user_location_table;

SELECT TOP 10 * FROM bronze_user_status_history_table;

-- CATEGORIES & PRODUCTS
SELECT TOP 10 * FROM bronze_categories_table;

SELECT TOP 10 * FROM bronze_product_catalog_table;

SELECT TOP 10 * FROM bronze_product_inventory_table;

SELECT TOP 10 * FROM bronze_product_pricing_history_table;


-- SESSIONS & CLICKSTREAM MODULE
SELECT TOP 10 * FROM bronze_session;

SELECT TOP 10 * FROM bronze_event_clickstream;


-- ORDERS & PAYMENTS
SELECT TOP 10 * FROM bronze_orders_table;

SELECT TOP 10 * FROM bronze_order_details_table;

SELECT TOP 10 * FROM bronze_order_status_history_table;

SELECT TOP 10 * FROM bronze_order_items_table;

SELECT TOP 10 * FROM bronze_order_item_pricing_table;

SELECT TOP 10 * FROM bronze_payment_details_table;

SELECT TOP 10 * FROM bronze_payment_status_history_table;