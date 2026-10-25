use InzynierkaDB;
GO

-- 1. Categories Table
CREATE TABLE silver_categories (
    category_surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    category_id VARCHAR(50) NOT NULL UNIQUE, 
    category_name VARCHAR(100) NOT NULL,
    parent_category_id VARCHAR(50),
    CONSTRAINT FK_Silver_Categories_Self FOREIGN KEY (parent_category_id) REFERENCES silver_categories(category_id)
);

-- 2. Products Table 
CREATE TABLE silver_products (
    product_surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    brand VARCHAR(100),
    description TEXT,
    base_price DECIMAL(10,2) NOT NULL,
    cost_price DECIMAL(10,2) NOT NULL,
    weight_g DECIMAL(10,2),
    length_cm DECIMAL(10,2),
    height_cm DECIMAL(10,2),
    width_cm DECIMAL(10,2),
    stock_quantity INT NOT NULL,
    created_at DATETIME NOT NULL,
    valid_from DATETIME NOT NULL,
    valid_to DATETIME,
    is_active INT NOT NULL,
    category_id VARCHAR(50) NOT NULL,
    CONSTRAINT FK_Silver_Products_Category FOREIGN KEY (category_id) REFERENCES silver_categories(category_id)
);

-- 3. Users Table 
CREATE TABLE silver_users (
    user_surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    user_id VARCHAR(10) NOT NULL, 
    customer_unique_id VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(150),
    phone VARCHAR(50),
    birth_date DATE,
    gender VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    status VARCHAR(50),
    created_at DATETIME NOT NULL,
    valid_from DATETIME NOT NULL,
    valid_to DATETIME,
    is_active INT NOT NULL
);

-- 4. Sessions Table
CREATE TABLE silver_sessions (
    session_surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL UNIQUE, 
    session_start DATETIME NOT NULL,
    session_end DATETIME,
    device_type VARCHAR(30),
    browser VARCHAR(53),
    traffic_source VARCHAR(30),
    user_surrogate_key INT NOT NULL, 
    CONSTRAINT FK_Silver_Sessions_User FOREIGN KEY (user_surrogate_key) REFERENCES silver_users(user_surrogate_key)
);

-- 5. Clickstream Events Table
CREATE TABLE silver_events_clickstream (
    event_surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    event_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time DATETIME NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50),
    CONSTRAINT FK_Silver_Clickstream_Session FOREIGN KEY (session_id) REFERENCES silver_sessions(session_id)
);

-- 6. Orders Table 
CREATE TABLE silver_orders (
    order_surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE, 
    total_amount DECIMAL(10,2) NOT NULL,
    freight_value DECIMAL(10,2) NOT NULL,
    shipping_method VARCHAR(30),
    created_at_timestamp DATETIME NOT NULL,
    pending_payment_timestamp DATETIME,
    paid_timestamp DATETIME,
    processing_timestamp DATETIME,
    shipped_timestamp DATETIME,
    delivered_timestamp DATETIME,
    session_id VARCHAR(50),
    user_surrogate_key INT,
    CONSTRAINT FK_Silver_Orders_Session FOREIGN KEY (session_id) REFERENCES silver_sessions(session_id),
    CONSTRAINT FK_Silver_Orders_User FOREIGN KEY (user_surrogate_key) REFERENCES silver_users(user_surrogate_key)
);

-- 7. Order Items Table
CREATE TABLE silver_order_items (
    order_item_surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    order_item_id VARCHAR(50) NOT NULL,
    item_sequence INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    CONSTRAINT FK_Silver_OrderItems_Order FOREIGN KEY (order_id) REFERENCES silver_orders(order_id)
);

-- 8. Payments Table
CREATE TABLE silver_payments (
    payment_surrogate_key INT IDENTITY(1,1) PRIMARY KEY,
    payment_id VARCHAR(50) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value DECIMAL(10,2) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    CONSTRAINT FK_Silver_Payments_Order FOREIGN KEY (order_id) REFERENCES silver_orders(order_id)
);
GO