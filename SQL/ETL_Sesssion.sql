USE InzynierkaDB;
GO

PRINT 'Loading Dim_Session from Silver Layer...';

INSERT INTO Dim_Session (
    session_id,
    session_start,
    session_end,
    session_duration_bucket,
    device_type,
    browser,
    traffic_source,
    conversion_status
)
SELECT 
    ss.session_id,
    ss.session_start,
    ss.session_end,
    
    -- 1. Session Duration Bucket Logic
    CASE 
        WHEN ss.session_end IS NULL THEN 'Bounce (< 10s)'
        WHEN DATEDIFF(SECOND, ss.session_start, ss.session_end) < 10 THEN 'Bounce (< 10s)'
        WHEN DATEDIFF(SECOND, ss.session_start, ss.session_end) BETWEEN 10 AND 59 THEN 'Brief (10s-60s)'
        WHEN DATEDIFF(SECOND, ss.session_start, ss.session_end) BETWEEN 60 AND 300 THEN 'Engaged (1m-5m)'
        ELSE 'Extended (> 5m)'
    END AS session_duration_bucket,
    
    ss.device_type,
    ss.browser,
    ss.traffic_source,
    
    -- 2. Dynamic Conversion Check (Scanning silver_events_clickstream for a checkout/purchase)
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM silver_events_clickstream se 
            WHERE se.session_id = ss.session_id 
              AND se.event_type IN ('Purchase', 'Checkout')
        ) THEN 'Converted (Purchase)'
        ELSE 'Browsing Only'
    END AS conversion_status

FROM silver_sessions ss
WHERE NOT EXISTS (
    SELECT 1 
    FROM Dim_Session ds 
    WHERE ds.session_id = ss.session_id
);

PRINT 'Dim_Session population complete!';
GO