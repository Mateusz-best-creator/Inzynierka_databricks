USE InzynierkaDB;
GO

-- ==========================================
-- 1. DROP FACT TABLES FIRST (due to Foreign Keys)
-- ==========================================
DROP TABLE IF EXISTS Fact_Events;
DROP TABLE IF EXISTS Fact_Sales;

-- ==========================================
-- 2. DROP DIMENSION TABLES
-- ==========================================
DROP TABLE IF EXISTS Dim_Date;
DROP TABLE IF EXISTS Dim_Time;
DROP TABLE IF EXISTS Dim_Geography;
DROP TABLE IF EXISTS Dim_User;
DROP TABLE IF EXISTS Dim_Product;
DROP TABLE IF EXISTS Dim_Session;
DROP TABLE IF EXISTS Dim_Event_Type;
DROP TABLE IF EXISTS Dim_Payment_Type;
DROP TABLE IF EXISTS Dim_Sales_Context;

PRINT 'All Gold layer tables dropped successfully!';
GO