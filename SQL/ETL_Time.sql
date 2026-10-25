USE InzynierkaDB;
GO

WITH Hours AS (
    SELECT TOP 24 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS Hour
    FROM sys.all_objects
),
Minutes AS (
    SELECT TOP 60 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS Minute
    FROM sys.all_objects
),
Seconds AS (
    SELECT TOP 60 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS Second
    FROM sys.all_objects
)
INSERT INTO Dim_Time ([hour], [minute], [seconds], day_part)
SELECT TOP 100 PERCENT
    h.Hour,
    m.Minute,
    s.Second,
    CASE 
        WHEN h.Hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN h.Hour BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN h.Hour BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS day_part
FROM Hours h
CROSS JOIN Minutes m
CROSS JOIN Seconds s
WHERE NOT EXISTS (
    SELECT 1 FROM Dim_Time dt 
    WHERE dt.[hour] = h.Hour 
      AND dt.[minute] = m.Minute 
      AND dt.[seconds] = s.Second
)
ORDER BY h.Hour, m.Minute, s.Second;
GO