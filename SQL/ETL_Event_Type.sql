USE InzynierkaDB;
GO

PRINT 'Loading Dim_Event_Type from Silver Layer...';

INSERT INTO Dim_Event_Type (
    event_type_name, 
    funnel_step_order, 
    funnel_stage_name
)
SELECT DISTINCT 
    src.event_type AS event_type_name,
    
    -- 1. Funnel Step Order Mapping (Adjust names to match your exact silver event types if needed)
    CASE 
        WHEN src.event_type IN ('View', 'PageView', 'ProductView') THEN 1
        WHEN src.event_type IN ('AddToCart', 'Cart', 'Wishlist') THEN 2
        WHEN src.event_type IN ('Checkout', 'Payment') THEN 3
        WHEN src.event_type IN ('Purchase', 'Complete') THEN 4
        ELSE 5 -- Catch-all for any other activity
    END AS funnel_step_order,
    
    -- 2. Funnel Stage Name Mapping
    CASE 
        WHEN src.event_type IN ('View', 'PageView', 'ProductView') THEN '1. Awareness (View)'
        WHEN src.event_type IN ('AddToCart', 'Cart', 'Wishlist') THEN '2. Consideration (Cart)'
        WHEN src.event_type IN ('Checkout', 'Payment', 'Purchase', 'Complete') THEN '3. Conversion (Purchase)'
        ELSE '4. Other / Unclassified'
    END AS funnel_stage_name

FROM silver_events_clickstream src
WHERE src.event_type IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 
    FROM Dim_Event_Type det 
    WHERE det.event_type_name = src.event_type
);

PRINT 'Dim_Event_Type population complete!';
GO