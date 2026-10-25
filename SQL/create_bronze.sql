use InzynierkaDB;
GO

-- USERS & LOCATIONS
CREATE TABLE bronze_location_table (
    location_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50)
);

CREATE TABLE bronze_users_table (
    user_id VARCHAR(10) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    created_at DATETIME
);

CREATE TABLE bronze_user_profile_table (
    user_id VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    birth_date DATE,
    gender VARCHAR(20),
    CONSTRAINT FK_Bronze_UserProfile_Users FOREIGN KEY (user_id) REFERENCES bronze_users_table(user_id)
);

CREATE TABLE bronze_user_contact_table (
    user_id VARCHAR(10) PRIMARY KEY,
    email VARCHAR(150),
    phone VARCHAR(50),
    CONSTRAINT FK_Bronze_UserContact_Users FOREIGN KEY (user_id) REFERENCES bronze_users_table(user_id)
);

CREATE TABLE bronze_user_location_table (
    id INT PRIMARY KEY,
    user_id VARCHAR(10),
    location_id VARCHAR(50),
    valid_from DATETIME,
    valid_to DATETIME,
    is_current INT,
    CONSTRAINT FK_Bronze_UserLocation_Users FOREIGN KEY (user_id) REFERENCES bronze_users_table(user_id),
    CONSTRAINT FK_Bronze_UserLocation_Locations FOREIGN KEY (location_id) REFERENCES bronze_location_table(location_id)
);

CREATE TABLE bronze_user_status_history_table (
    id VARCHAR(10) PRIMARY KEY,
    user_id VARCHAR(10),
    status VARCHAR(50),
    status_change_description VARCHAR(255),
    valid_from DATETIME,
    valid_to DATETIME,
    is_current INT,
    CONSTRAINT FK_Bronze_UserStatus_Users FOREIGN KEY (user_id) REFERENCES bronze_users_table(user_id)
);


-- CATEGORIES & PRODUCTS
CREATE TABLE bronze_categories_table (
    category_id VARCHAR(50) PRIMARY KEY,
    parent_category_id VARCHAR(50),
    category_name VARCHAR(100),
    created_at DATETIME
);

CREATE TABLE bronze_product_catalog_table (
    id VARCHAR(10) PRIMARY KEY,
    product_id VARCHAR(50),
    category_id VARCHAR(50),
    product_name VARCHAR(150),
    brand VARCHAR(100),
    description VARCHAR(MAX),
    is_active INT,
    valid_from DATETIME,
    valid_to DATETIME,
    is_current INT,
    CONSTRAINT FK_Bronze_ProductCatalog_Category FOREIGN KEY (category_id) REFERENCES bronze_categories_table(category_id)
);

CREATE TABLE bronze_product_inventory_table (
    inventory_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50),
    weight_g DECIMAL(10,2),
    length_cm DECIMAL(10,2),
    height_cm DECIMAL(10,2),
    width_cm DECIMAL(10,2),
    stock_quantity INT,
    valid_from DATETIME,
    valid_to DATETIME,
    is_current INT
);

CREATE TABLE bronze_product_pricing_history_table (
    unique_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50),
    price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    valid_from DATETIME,
    valid_to DATETIME,
    is_current INT
);


-- SESSIONS & CLICKSTREAM MODULE
CREATE TABLE bronze_session (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(10),
    session_start DATETIME,
    session_end DATETIME,
    device_type VARCHAR(30),
    browser VARCHAR(100),
    traffic_source VARCHAR(30),
    CONSTRAINT FK_Bronze_Session_Users FOREIGN KEY (user_id) REFERENCES bronze_users_table(user_id)
);

CREATE TABLE bronze_event_clickstream (
    session_id VARCHAR(50),
    user_id VARCHAR(10),
    event_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50),
    event_time DATETIME,
    event_type VARCHAR(50),
    CONSTRAINT FK_Bronze_Clickstream_Session FOREIGN KEY (session_id) REFERENCES bronze_session(session_id),
    CONSTRAINT FK_Bronze_Clickstream_Users FOREIGN KEY (user_id) REFERENCES bronze_users_table(user_id)
);


-- ORDERS & PAYMENTS
CREATE TABLE bronze_orders_table (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(10),
    session_id VARCHAR(50),
    created_at DATETIME,
    CONSTRAINT FK_Bronze_Orders_Users FOREIGN KEY (user_id) REFERENCES bronze_users_table(user_id),
    CONSTRAINT FK_Bronze_Orders_Session FOREIGN KEY (session_id) REFERENCES bronze_session(session_id)
);

CREATE TABLE bronze_order_details_table (
    order_id VARCHAR(50) PRIMARY KEY,
    order_channel VARCHAR(30),
    shipping_method VARCHAR(30),
    freight_value DECIMAL(10,2),
    CONSTRAINT FK_Bronze_OrderDetails_Order FOREIGN KEY (order_id) REFERENCES bronze_orders_table(order_id)
);

CREATE TABLE bronze_order_status_history_table (
    id VARCHAR(10) PRIMARY KEY,
    order_id VARCHAR(50),
    status VARCHAR(50),
    status_change_description VARCHAR(255),
    valid_from DATETIME,
    valid_to DATETIME,
    is_current INT,
    CONSTRAINT FK_Bronze_OrderStatus_Order FOREIGN KEY (order_id) REFERENCES bronze_orders_table(order_id)
);

CREATE TABLE bronze_order_items_table (
    order_item_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    order_item_seq INT,
    product_id VARCHAR(50),
    quantity INT,
    CONSTRAINT FK_Bronze_OrderItems_Order FOREIGN KEY (order_id) REFERENCES bronze_orders_table(order_id)
);

CREATE TABLE bronze_order_item_pricing_table (
    order_item_id VARCHAR(50) PRIMARY KEY,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2),
    CONSTRAINT FK_Bronze_ItemPricing_Item FOREIGN KEY (order_item_id) REFERENCES bronze_order_items_table(order_item_id)
);

CREATE TABLE bronze_payment_details_table (
    payment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2),
    CONSTRAINT FK_Bronze_PaymentDetails_Order FOREIGN KEY (order_id) REFERENCES bronze_orders_table(order_id)
);

CREATE TABLE bronze_payment_status_history_table (
    log_id VARCHAR(50) PRIMARY KEY,
    payment_id VARCHAR(50),
    status VARCHAR(50),
    gateway_response VARCHAR(100),
    valid_from DATETIME,
    valid_to DATETIME,
    is_current INT,
    CONSTRAINT FK_Bronze_PaymentStatus_Payment FOREIGN KEY (payment_id) REFERENCES bronze_payment_details_table(payment_id)
);