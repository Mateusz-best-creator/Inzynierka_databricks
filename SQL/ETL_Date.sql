USE InzynierkaDB;
GO

-- Set the first day of the week to Monday (1 = Monday, 7 = Sunday)
SET DATEFIRST 1;

DECLARE @StartDate DATE = '2022-01-01';
DECLARE @EndDate DATE = '2026-12-31';
DECLARE @DateInProcess DATETIME = @StartDate;

WHILE @DateInProcess <= @EndDate
BEGIN
    -- Check if the date already exists to maintain idempotency
    IF NOT EXISTS (SELECT 1 FROM Dim_Date WHERE full_date = @DateInProcess)
    BEGIN
        INSERT INTO Dim_Date (
            full_date, 
            [day], 
            [month], 
            [quarter], 
            [year], 
            month_name, 
            day_of_month, 
            day_of_week, 
            is_weekend
        )
        VALUES (
            @DateInProcess,
            DAY(@DateInProcess),                                           -- [day]
            MONTH(@DateInProcess),                                         -- [month]
            'Q' + CAST(DATEPART(QUARTER, @DateInProcess) AS VARCHAR(10)),  -- [quarter] (e.g., 'Q1', 'Q2')
            YEAR(@DateInProcess),                                          -- [year]
            DATENAME(MONTH, @DateInProcess),                               -- month_name (e.g., 'January')
            DAY(@DateInProcess),                                           -- day_of_month
            DATEPART(WEEKDAY, @DateInProcess),                             -- day_of_week (1 = Monday through 7 = Sunday)
            CASE WHEN DATENAME(WEEKDAY, @DateInProcess) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END -- is_weekend
        );
    END

    SET @DateInProcess = DATEADD(DAY, 1, @DateInProcess);
END

-- Optional: Reset DATEFIRST back to server default (usually 7 for US English) if needed
GO