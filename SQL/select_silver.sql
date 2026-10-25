USE InzynierkaDB;
GO

SELECT TOP 10 * FROM silver_categories;

SELECT * FROM silver_products where product_id='PRD-0000000001';


SELECT * FROM silver_users WHERE user_surrogate_key='13031'
SELECT TOP 15 * FROM silver_users;

SELECT TOP 10 * FROM silver_sessions;

SELECT TOP 10 * FROM silver_events_clickstream;

SELECT TOP 10 * FROM silver_orders WHERE order_id='ORDER-0000000081';

SELECT TOP 10 * FROM silver_order_items;

SELECT * FROM silver_payments WHERE payment_sequential>1 AND payment_installments>1;
SELECT * FROM silver_payments WHERE order_id='ORDER-0000008386';