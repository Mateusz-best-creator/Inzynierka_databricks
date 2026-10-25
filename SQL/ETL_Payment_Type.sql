USE InzynierkaDB;
GO

PRINT 'Loading Dim_Payment_Type from Silver Layer...';

INSERT INTO Dim_Payment_Type (
    payment_type_name,
    installment_tier
)
SELECT DISTINCT 
    src.payment_type AS payment_type_name,
    
    -- Installment Tier Mapping Logic
    CASE 
        WHEN ISNULL(src.payment_installments, 1) <= 1 THEN 'Single Payment'
        WHEN src.payment_installments BETWEEN 2 AND 3 THEN '2-3 Installments'
        ELSE '4+ Installments'
    END AS installment_tier

FROM silver_payments src
WHERE src.payment_type IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 
    FROM Dim_Payment_Type dpt 
    WHERE dpt.payment_type_name = src.payment_type
      AND dpt.installment_tier = CASE 
            WHEN ISNULL(src.payment_installments, 1) <= 1 THEN 'Single Payment'
            WHEN src.payment_installments BETWEEN 2 AND 3 THEN '2-3 Installments'
            ELSE '4+ Installments'
        END
  );

PRINT 'Dim_Payment_Type population complete!';
GO