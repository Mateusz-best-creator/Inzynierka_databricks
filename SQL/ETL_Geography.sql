USE InzynierkaDB;
GO

INSERT INTO Dim_Geography (zip_code, city, state, country)
SELECT DISTINCT 
    su.zip_code,
    su.city,
    su.state,
    su.country
FROM silver_users su
WHERE su.city IS NOT NULL 
  AND su.state IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 
    FROM Dim_Geography dg
    WHERE ISNULL(dg.zip_code, '') = ISNULL(su.zip_code, '')
      AND dg.city = su.city
      AND dg.state = su.state
      AND dg.country = su.country
);
GO