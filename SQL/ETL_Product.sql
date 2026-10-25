USE InzynierkaDB;
GO

PRINT 'Loading Dim_Product from Silver Layer...';

INSERT INTO Dim_Product (
    product_id,
    product_name,
    brand,
    description,
    category_id,
    category_name,
    parent_category_id,
    parent_category_name,
    price_tier,
    margin_tier,
    weight_class,
    package_size_tier,
    stock_status,
    product_lifecycle_status,
    created_at,
    valid_from,
    valid_to,
    scd_record_status
)
SELECT 
    sp.product_id,
    sp.product_name,
    sp.brand,
    sp.description,
    sp.category_id,
    ISNULL(sc.category_name, 'Unknown') AS category_name,
    sc.parent_category_id,
    -- Self-join lookup for parent category name (defaults to 'Root / None' if parent_id is NULL)
    ISNULL(pc.category_name, 'Root / None') AS parent_category_name,
    
    -- 1. Price Tier Logic
    CASE 
        WHEN sp.base_price < 50 THEN 'Budget (< 50)'
        WHEN sp.base_price BETWEEN 50 AND 200 THEN 'Regular (50-200)'
        WHEN sp.base_price BETWEEN 200 AND 500 THEN 'Premium (200-500)'
        ELSE 'Luxury (> 500)'
    END AS price_tier,
    
    -- 2. Margin Tier Logic
    CASE 
        WHEN sp.base_price IS NULL OR sp.base_price = 0 THEN 'Low Margin (< 15%)'
        WHEN ((sp.base_price - sp.cost_price) / sp.base_price) < 0.15 THEN 'Low Margin (< 15%)'
        WHEN ((sp.base_price - sp.cost_price) / sp.base_price) BETWEEN 0.15 AND 0.35 THEN 'Standard Margin (15-35%)'
        ELSE 'High Margin (> 35%)'
    END AS margin_tier,
    
    -- 3. Weight Class Logic
    CASE 
        WHEN ISNULL(sp.weight_g, 0) < 500 THEN 'Light (< 0.5 kg)'
        WHEN sp.weight_g BETWEEN 500 AND 5000 THEN 'Standard (0.5-5 kg)'
        WHEN sp.weight_g BETWEEN 5001 AND 20000 THEN 'Heavy (5-20 kg)'
        ELSE 'Freight (> 20 kg)'
    END AS weight_class,
    
    -- 4. Package Size Tier Logic
    CASE 
        WHEN (ISNULL(sp.length_cm, 0) * ISNULL(sp.width_cm, 0) * ISNULL(sp.height_cm, 0)) < 1000 THEN 'Small (Parcel A)'
        WHEN (sp.length_cm * sp.width_cm * sp.height_cm) BETWEEN 1000 AND 5000 THEN 'Medium (Parcel B)'
        WHEN (sp.length_cm * sp.width_cm * sp.height_cm) BETWEEN 5001 AND 20000 THEN 'Large (Parcel C)'
        ELSE 'Oversized'
    END AS package_size_tier,
    
    -- 5. Stock Status Logic
    CASE 
        WHEN ISNULL(sp.stock_quantity, 0) = 0 THEN 'Out of Stock (0)'
        WHEN sp.stock_quantity BETWEEN 1 AND 10 THEN 'Low Stock (1-10)'
        WHEN sp.stock_quantity BETWEEN 11 AND 100 THEN 'In Stock (11-100)'
        ELSE 'High Stock (> 100)'
    END AS stock_status,
    
    -- 6. Product Lifecycle Status
    CASE 
        WHEN sp.is_active = 1 THEN 'Active Catalog'
        ELSE 'Discontinued'
    END AS product_lifecycle_status,
    
    sp.created_at,
    sp.valid_from,
    sp.valid_to,
    
    -- 7. SCD2 Record Status Mapping
    CASE 
        WHEN sp.valid_to IS NULL THEN 'Current'
        ELSE 'Historical'
    END AS scd_record_status

FROM silver_products sp
-- Join 1: Get the product's direct category
LEFT JOIN silver_categories sc ON sp.category_id = sc.category_id
-- Join 2: Self-join to get the parent category's name using the parent_category_id
LEFT JOIN silver_categories pc ON sc.parent_category_id = pc.category_id
WHERE NOT EXISTS (
    SELECT 1 
    FROM Dim_Product dp 
    WHERE dp.product_id = sp.product_id 
      AND dp.valid_from = sp.valid_from
);

PRINT 'Dim_Product population complete!';
GO