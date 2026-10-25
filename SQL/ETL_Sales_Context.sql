USE InzynierkaDB;
GO

PRINT 'Loading Dim_Sales_Context from Silver Layer...';

INSERT INTO Dim_Sales_Context (
    shipping_method,
    discount_status,
    delivery_sla_status
)
SELECT DISTINCT 
    ISNULL(src.shipping_method, 'Standard') AS shipping_method,
    
    -- 1. Discount Status Logic
    CASE 
        WHEN ISNULL(src.discount_amount, 0) > 0 THEN 'Discounted'
        ELSE 'Full Price'
    END AS discount_status,
    
    -- 2. Delivery SLA Status Logic (Comparing actual vs estimated delivery dates)
    CASE 
        WHEN src.actual_delivery_date IS NULL OR src.estimated_delivery_date IS NULL THEN 'On-Time Delivery' -- Safe fallback
        WHEN src.actual_delivery_date <= src.estimated_delivery_date THEN 'On-Time Delivery'
        ELSE 'Delayed Delivery'
    END AS delivery_sla_status

FROM silver_orders src -- Adjust to your exact Silver orders/fulfillment table name if different
WHERE NOT EXISTS (
    SELECT 1 
    FROM Dim_Sales_Context dsc 
    WHERE dsc.shipping_method = ISNULL(src.shipping_method, 'Standard')
      AND dsc.discount_status = CASE 
            WHEN ISNULL(src.discount_amount, 0) > 0 THEN 'Discounted' 
            ELSE 'Full Price' 
        END
      AND dsc.delivery_sla_status = CASE 
            WHEN src.actual_delivery_date IS NULL OR src.estimated_delivery_date IS NULL THEN 'On-Time Delivery'
            WHEN src.actual_delivery_date <= src.estimated_delivery_date THEN 'On-Time Delivery'
            ELSE 'Delayed Delivery'
        END
);

PRINT 'Dim_Sales_Context population complete!';
GO