USE InzynierkaDB;
GO

-- ==========================================
-- 1. DIMENSION TABLES
-- ==========================================
CREATE TABLE Dim_Date (
    date_key INT IDENTITY(1,1) PRIMARY KEY,
    full_date DATE NOT NULL,
    [day] INT NOT NULL,
    [month] INT NOT NULL,
    [quarter] VARCHAR(50) NOT NULL,
    [year] INT NOT NULL,
    month_name VARCHAR(50) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BIT NOT NULL
);

CREATE TABLE Dim_Time (
    time_key INT IDENTITY(1,1) PRIMARY KEY,
    [hour] INT NOT NULL,
    [minute] INT NOT NULL,
    [seconds] INT NOT NULL,
    day_part VARCHAR(50) NOT NULL
);

CREATE TABLE Dim_Geography (
    geography_key INT IDENTITY(1,1) PRIMARY KEY, -- Changed to INT for consistency with FK types
    zip_code VARCHAR(20),
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL
);

CREATE TABLE Dim_User (
    user_surrogate_key INT PRIMARY KEY,
    user_id VARCHAR(10) NOT NULL,
    customer_unique_id VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(150),
    phone VARCHAR(50),
    birth_date DATE,
    age_group VARCHAR(20) NOT NULL,
    gender VARCHAR(20),
    status VARCHAR(50),
    created_at DATETIME NOT NULL,
    valid_from DATETIME NOT NULL,
    valid_to DATETIME,
    scd_record_status VARCHAR(15) NOT NULL
);

CREATE TABLE Dim_Product (
    product_surrogate_key BIGINT IDENTITY(1,1) PRIMARY KEY, -- Renamed to product_surrogate_key for FK clarity
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    brand VARCHAR(100),
    description TEXT,
    category_id VARCHAR(50),
    category_name VARCHAR(100),
    parent_category_id VARCHAR(50),
    parent_category_name VARCHAR(100),
    price_tier VARCHAR(30) NOT NULL,
    margin_tier VARCHAR(30) NOT NULL,
    weight_class VARCHAR(30) NOT NULL,
    package_size_tier VARCHAR(30) NOT NULL,
    stock_status VARCHAR(30) NOT NULL,
    product_lifecycle_status VARCHAR(15) NOT NULL,
    created_at DATETIME NOT NULL,
    valid_from DATETIME NOT NULL,
    valid_to DATETIME,
    scd_record_status VARCHAR(15) NOT NULL
);

CREATE TABLE Dim_Session (
    session_surrogate_key BIGINT IDENTITY(1,1) PRIMARY KEY, -- Renamed to session_surrogate_key for FK clarity
    session_id VARCHAR(50) NOT NULL,
    session_start DATETIME NOT NULL,
    session_end DATETIME,
    session_duration_bucket VARCHAR(30) NOT NULL,
    device_type VARCHAR(30),
    browser VARCHAR(53),
    traffic_source VARCHAR(30),
    conversion_status VARCHAR(30) NOT NULL
);

CREATE TABLE Dim_Event_Type (
    event_type_key INT IDENTITY(1,1) PRIMARY KEY,
    event_type_name VARCHAR(50) NOT NULL,
    funnel_step_order INT NOT NULL,
    funnel_stage_name VARCHAR(50) NOT NULL
);

CREATE TABLE Dim_Payment_Type (
    payment_type_key INT IDENTITY(1,1) PRIMARY KEY,
    payment_type_name VARCHAR(50) NOT NULL,
    installment_tier VARCHAR(30) NOT NULL
);

CREATE TABLE Dim_Sales_Context (
    sales_context_key INT IDENTITY(1,1) PRIMARY KEY,
    shipping_method VARCHAR(30) NOT NULL,
    discount_status VARCHAR(20) NOT NULL,
    delivery_sla_status VARCHAR(25) NOT NULL
);
GO

-- ==========================================
-- 2. FACT TABLES (WITH FOREIGN KEYS)
-- ==========================================

-- Fact_Events (Clickstream Analytics)
CREATE TABLE Fact_Events (
    event_fact_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign Keys to Dimensions
    event_date_key INT NOT NULL,
    event_time_key INT NOT NULL,
    session_surrogate_key BIGINT NOT NULL,
    user_surrogate_key INT NOT NULL,
    geography_key INT,
    product_surrogate_key BIGINT NOT NULL,
    event_type_key INT NOT NULL,
    
    -- Degenerate Dimensions
    event_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    
    -- Behavioral & Funnel Measures
    event_sequence_in_session INT NOT NULL,
    time_on_step_seconds INT,
    product_price_at_event DECIMAL(10,2),
    product_stock_at_event INT,

    -- Constraints / Foreign Keys
    CONSTRAINT FK_FactEvents_Date FOREIGN KEY (event_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT FK_FactEvents_Time FOREIGN KEY (event_time_key) REFERENCES Dim_Time(time_key),
    CONSTRAINT FK_FactEvents_Session FOREIGN KEY (session_surrogate_key) REFERENCES Dim_Session(session_surrogate_key),
    CONSTRAINT FK_FactEvents_User FOREIGN KEY (user_surrogate_key) REFERENCES Dim_User(user_surrogate_key),
    CONSTRAINT FK_FactEvents_Geography FOREIGN KEY (geography_key) REFERENCES Dim_Geography(geography_key),
    CONSTRAINT FK_FactEvents_Product FOREIGN KEY (product_surrogate_key) REFERENCES Dim_Product(product_surrogate_key),
    CONSTRAINT FK_FactEvents_EventType FOREIGN KEY (event_type_key) REFERENCES Dim_Event_Type(event_type_key)
);

-- Fact_Sales (Line-Item Grain Commercial Transactions)
CREATE TABLE Fact_Sales (
    sales_fact_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign Keys to Dimensions
    order_date_key INT NOT NULL,
    order_time_key INT NOT NULL,
    paid_date_key INT,
    shipped_date_key INT,
    delivered_date_key INT,
    user_surrogate_key INT,
    geography_key INT,
    product_surrogate_key BIGINT NOT NULL,
    session_surrogate_key BIGINT,
    payment_type_key INT,
    sales_context_key INT NOT NULL,
    
    -- Degenerate Dimensions
    order_id VARCHAR(50) NOT NULL,
    item_sequence INT NOT NULL,
    
    -- Additive Financial & Quantity Measures
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    unit_cost_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) NOT NULL,
    gross_item_amount DECIMAL(10,2) NOT NULL,
    net_item_amount DECIMAL(10,2) NOT NULL,
    freight_value DECIMAL(10,2) NOT NULL,
    total_line_amount DECIMAL(10,2) NOT NULL,
    net_profit_margin DECIMAL(10,2) NOT NULL,
    
    -- Additive Physical Logistics Measures
    total_item_weight_g DECIMAL(10,2) NOT NULL,
    total_item_volume_cm3 DECIMAL(12,2) NOT NULL,
    
    -- Payment Context Measures
    payment_installments_count INT,
    payment_methods_used_count INT,
    
    -- Fulfillment Lag / SLA Measures
    minutes_to_pay INT,
    hours_to_process INT,
    hours_to_ship INT,
    hours_to_deliver INT,
    total_fulfillment_days DECIMAL(10,2),

    -- Constraints / Foreign Keys
    CONSTRAINT FK_FactSales_OrderDate FOREIGN KEY (order_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT FK_FactSales_OrderTime FOREIGN KEY (order_time_key) REFERENCES Dim_Time(time_key),
    CONSTRAINT FK_FactSales_PaidDate FOREIGN KEY (paid_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT FK_FactSales_ShippedDate FOREIGN KEY (shipped_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT FK_FactSales_DeliveredDate FOREIGN KEY (delivered_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT FK_FactSales_User FOREIGN KEY (user_surrogate_key) REFERENCES Dim_User(user_surrogate_key),
    CONSTRAINT FK_FactSales_Geography FOREIGN KEY (geography_key) REFERENCES Dim_Geography(geography_key),
    CONSTRAINT FK_FactSales_Product FOREIGN KEY (product_surrogate_key) REFERENCES Dim_Product(product_surrogate_key),
    CONSTRAINT FK_FactSales_Session FOREIGN KEY (session_surrogate_key) REFERENCES Dim_Session(session_surrogate_key),
    CONSTRAINT FK_FactSales_PaymentType FOREIGN KEY (payment_type_key) REFERENCES Dim_Payment_Type(payment_type_key),
    CONSTRAINT FK_FactSales_SalesContext FOREIGN KEY (sales_context_key) REFERENCES Dim_Sales_Context(sales_context_key)
);
GO