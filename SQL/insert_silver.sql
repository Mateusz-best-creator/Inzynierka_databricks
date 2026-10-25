USE InzynierkaDB;
GO

-- 1. Load Categories
PRINT 'Loading silver_categories...';
INSERT INTO silver_categories (category_id, category_name, parent_category_id)
SELECT 
    category_id,
    category_name,
    parent_category_id
FROM bronze_categories_table;


-- 2. Load Products (Combines catalog, current pricing, and current inventory)
PRINT 'Loading silver_products...';
WITH ProductTimelines AS (
    -- Collect all unique boundary timestamps from catalog, pricing, and inventory SCD2 timelines
    SELECT product_id, valid_from AS timeline_point FROM bronze_product_catalog_table
    UNION
    SELECT product_id, valid_to FROM bronze_product_catalog_table WHERE valid_to IS NOT NULL
    UNION
    SELECT product_id, valid_from FROM bronze_product_pricing_history_table
    UNION
    SELECT product_id, valid_to FROM bronze_product_pricing_history_table WHERE valid_to IS NOT NULL
    UNION
    SELECT product_id, valid_from FROM bronze_product_inventory_table
    UNION
    SELECT product_id, valid_to FROM bronze_product_inventory_table WHERE valid_to IS NOT NULL
),
OrderedTimelines AS (
    -- Create continuous, non-overlapping time intervals per product
    SELECT 
        product_id,
        timeline_point AS valid_from,
        LEAD(timeline_point) OVER (PARTITION BY product_id ORDER BY timeline_point) AS valid_to
    FROM (
        SELECT DISTINCT product_id, timeline_point FROM ProductTimelines
    ) t
)
INSERT INTO silver_products (
    product_id, product_name, brand, description, base_price, cost_price, 
    weight_g, length_cm, height_cm, width_cm, stock_quantity, 
    created_at, valid_from, valid_to, is_active, category_id
)
SELECT 
    c.product_id,
    c.product_name,
    c.brand,
    c.description,
    ABS(ISNULL(p.price, 0.00)) AS base_price,
    ABS(ISNULL(p.cost_price, 0.00)) AS cost_price,
    i.weight_g,
    i.length_cm,
    i.height_cm,
    i.width_cm,
    ISNULL(i.stock_quantity, 0) AS stock_quantity,
    (SELECT MIN(valid_from) FROM bronze_product_catalog_table WHERE product_id = ot.product_id) AS created_at,
    ot.valid_from,
    ot.valid_to,
    c.is_active,
    c.category_id
FROM OrderedTimelines ot
-- Match product catalog version active during this exact time window
JOIN bronze_product_catalog_table c 
    ON ot.product_id = c.product_id 
    AND ot.valid_from >= c.valid_from 
    AND (ot.valid_from < c.valid_to OR c.valid_to IS NULL)
-- Match price active during this exact time window
LEFT JOIN bronze_product_pricing_history_table p 
    ON ot.product_id = p.product_id 
    AND ot.valid_from >= p.valid_from 
    AND (ot.valid_from < p.valid_to OR p.valid_to IS NULL)
-- Match inventory active during this exact time window
LEFT JOIN bronze_product_inventory_table i 
    ON ot.product_id = i.product_id 
    AND ot.valid_from >= i.valid_from 
    AND (ot.valid_from < i.valid_to OR i.valid_to IS NULL)
WHERE ot.valid_from IS NOT NULL;


-- 3. Load Users (Combines users, profiles, contacts, and current location)
PRINT 'Loading silver_users...';
WITH UserTimelines AS (
    SELECT user_id, valid_from AS timeline_point FROM bronze_user_location_table
    UNION
    SELECT user_id, valid_to FROM bronze_user_location_table WHERE valid_to IS NOT NULL
    UNION
    SELECT user_id, valid_from FROM bronze_user_status_history_table
    UNION
    SELECT user_id, valid_to FROM bronze_user_status_history_table WHERE valid_to IS NOT NULL
    UNION
    SELECT user_id, created_at AS timeline_point FROM bronze_users_table
),
OrderedUserTimelines AS (
    SELECT 
        user_id,
        timeline_point AS valid_from,
        LEAD(timeline_point) OVER (PARTITION BY user_id ORDER BY timeline_point) AS valid_to
    FROM (
        SELECT DISTINCT user_id, timeline_point FROM UserTimelines
    ) t
)
INSERT INTO silver_users (
    user_id, customer_unique_id, first_name, last_name, email, phone, 
    birth_date, gender, city, state, zip_code, country, status, 
    created_at, valid_from, valid_to, is_active
)
SELECT 
    u.user_id,
    u.customer_unique_id,
    pr.first_name,
    pr.last_name,
    co.email,
    co.phone,
    pr.birth_date,
    pr.gender,
    l.city,
    l.state,
    l.zip_code,
    l.country,
    ISNULL(s.status, 'ACTIVE') AS status,
    u.created_at,
    ot.valid_from,
    ot.valid_to,
    -- Account is active ONLY if status is explicitly 'ACTIVE' during this interval
    CASE WHEN ISNULL(s.status, 'ACTIVE') = 'ACTIVE' THEN 1 ELSE 0 END AS is_active
FROM OrderedUserTimelines ot
JOIN bronze_users_table u ON ot.user_id = u.user_id
LEFT JOIN bronze_user_profile_table pr ON ot.user_id = pr.user_id
LEFT JOIN bronze_user_contact_table co ON ot.user_id = co.user_id
LEFT JOIN bronze_user_location_table ul ON ot.user_id = ul.user_id 
    AND ot.valid_from >= ul.valid_from 
    AND (ot.valid_from < ul.valid_to OR ul.valid_to IS NULL)
LEFT JOIN bronze_location_table l ON ul.location_id = l.location_id
LEFT JOIN bronze_user_status_history_table s ON ot.user_id = s.user_id 
    AND ot.valid_from >= s.valid_from 
    AND (ot.valid_from < s.valid_to OR s.valid_to IS NULL)
WHERE ot.valid_from IS NOT NULL;


-- 4. Load Sessions
PRINT 'Loading silver_sessions...';
INSERT INTO silver_sessions (
    session_id, 
    session_start, 
    session_end, 
    device_type, 
    browser, 
    traffic_source, 
    user_surrogate_key
)
SELECT 
    ses.session_id,
    ses.session_start,
    ses.session_end,
    ses.device_type,
    ses.browser,
    ses.traffic_source,
    u.user_surrogate_key
FROM bronze_session ses
INNER JOIN silver_users u 
    ON ses.user_id = u.user_id 
    AND ses.session_start >= u.valid_from 
    AND (ses.session_start < u.valid_to OR u.valid_to IS NULL);


-- 5. Load Orders
PRINT 'Loading silver_orders...';
INSERT INTO silver_orders (
    order_id, 
    total_amount, 
    freight_value, 
    shipping_method, 
    created_at_timestamp, 
    pending_payment_timestamp, 
    paid_timestamp, 
    processing_timestamp, 
    shipped_timestamp, 
    delivered_timestamp, 
    session_id, 
    user_surrogate_key
)
SELECT 
    o.order_id,
    -- Total amount is now (Sum of Discounted Line Items) + Freight Value
    ISNULL(item_totals.items_amt, 0.00) + ISNULL(od.freight_value, 0.00) AS total_amount,
    ISNULL(od.freight_value, 0.00) AS freight_value,
    od.shipping_method,
    o.created_at AS created_at_timestamp,
    MAX(CASE WHEN osh.status = 'PENDING_PAYMENT' THEN osh.valid_from END) AS pending_payment_timestamp,
    MAX(CASE WHEN osh.status = 'PAID' THEN osh.valid_from END) AS paid_timestamp,
    MAX(CASE WHEN osh.status = 'PROCESSING' THEN osh.valid_from END) AS processing_timestamp,
    MAX(CASE WHEN osh.status = 'SHIPPED' THEN osh.valid_from END) AS shipped_timestamp,
    MAX(CASE WHEN osh.status = 'DELIVERED' THEN osh.valid_from END) AS delivered_timestamp,
    o.session_id,
    u.user_surrogate_key
FROM bronze_orders_table o
LEFT JOIN bronze_order_details_table od ON o.order_id = od.order_id
LEFT JOIN bronze_order_status_history_table osh ON o.order_id = osh.order_id
-- Subquery calculating net items total (quantity * [unit_price - discount])
LEFT JOIN (
    SELECT 
        oi.order_id, 
        SUM(CAST(oi.quantity AS DECIMAL(10,2)) * (ip.unit_price - ISNULL(ip.discount, 0.00))) AS items_amt
    FROM bronze_order_items_table oi
    JOIN bronze_order_item_pricing_table ip ON oi.order_item_id = ip.order_item_id
    GROUP BY oi.order_id
) item_totals ON o.order_id = item_totals.order_id
-- Match the version of the user active when the order was created
LEFT JOIN silver_users u 
    ON o.user_id = u.user_id 
    AND o.created_at >= u.valid_from 
    AND (o.created_at < u.valid_to OR u.valid_to IS NULL)
GROUP BY 
    o.order_id, 
    item_totals.items_amt, 
    od.freight_value, 
    od.shipping_method, 
    o.created_at, 
    o.session_id, 
    u.user_surrogate_key;


-- 6. Load Order Items tutaj surogate key jako klucz obcy 
PRINT 'Loading silver_order_items...';
INSERT INTO silver_order_items (
    order_item_id, 
    item_sequence, 
    quantity, 
    unit_price, 
    discount,
    order_id, 
    product_id
)
SELECT 
    oi.order_item_id,
    oi.order_item_seq,
    oi.quantity,
    ABS(ISNULL(ip.unit_price, 0.00)) AS unit_price,
    ABS(ISNULL(ip.discount, 0.00)) AS discount,
    oi.order_id,
    oi.product_id
FROM bronze_order_items_table oi
LEFT JOIN bronze_order_item_pricing_table ip ON oi.order_item_id = ip.order_item_id;


-- 7. Load Payments
PRINT 'Loading silver_payments...';
INSERT INTO silver_payments (
    payment_id, 
    payment_sequential, 
    payment_type, 
    payment_installments, 
    payment_value, 
    order_id
)
SELECT 
    payment_id,
    payment_sequential,
    payment_type,
    payment_installments,
    ABS(ISNULL(payment_value, 0.00)) AS payment_value,
    order_id
FROM bronze_payment_details_table;


-- 8. Load Clickstream Events
PRINT 'Loading silver_events_clickstream...';
INSERT INTO silver_events_clickstream (
    event_id, 
    event_type, 
    event_time, 
    session_id, 
    product_id
)
SELECT 
    event_id,
    event_type,
    event_time,
    session_id,
    -- Convert empty strings to NULL for clean analytics and foreign key handling
    CASE WHEN product_id = '' OR product_id IS NULL THEN NULL ELSE product_id END AS product_id
FROM bronze_event_clickstream;

PRINT 'Silver layer population complete!';