USE InzynierkaDB;
GO

INSERT INTO Dim_User (
    user_surrogate_key,
    user_id,
    customer_unique_id,
    first_name,
    last_name,
    email,
    phone,
    birth_date,
    age_group,
    gender,
    status,
    created_at,
    valid_from,
    valid_to,
    scd_record_status
)
SELECT 
    su.user_surrogate_key,
    su.user_id,
    su.customer_unique_id,
    su.first_name,
    su.last_name,
    su.email,
    su.phone,
    su.birth_date,
    -- Calculate analytical age group brackets based on birth date
    CASE 
        WHEN su.birth_date IS NULL THEN 'Unknown'
        WHEN DATEDIFF(YEAR, su.birth_date, GETDATE()) BETWEEN 18 AND 24 THEN '18-24'
        WHEN DATEDIFF(YEAR, su.birth_date, GETDATE()) BETWEEN 25 AND 34 THEN '25-34'
        WHEN DATEDIFF(YEAR, su.birth_date, GETDATE()) BETWEEN 35 AND 49 THEN '35-49'
        WHEN DATEDIFF(YEAR, su.birth_date, GETDATE()) >= 50 THEN '50+'
        ELSE 'Unknown'
    END AS age_group,
    su.gender,
    su.status,
    su.created_at,
    su.valid_from,
    su.valid_to,
    -- Determine SCD2 record status flag
    CASE 
        WHEN su.valid_to IS NULL THEN 'Current' 
        ELSE 'Historical' 
    END AS scd_record_status
FROM silver_users su
WHERE NOT EXISTS (
    SELECT 1 
    FROM Dim_User du 
    WHERE du.user_surrogate_key = su.user_surrogate_key
);

PRINT 'Dim_User population complete!';
GO