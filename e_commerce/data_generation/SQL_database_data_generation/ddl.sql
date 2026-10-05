
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(10) PRIMARY KEY,
    customer_unique_id INT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_profile (
    user_id VARCHAR(10) PRIMARY KEY,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    first_name VARCHAR(64),
    last_name VARCHAR(64),
    birth_date DATE,
    gender VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS user_contact (
    user_id VARCHAR(10) PRIMARY KEY,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    email VARCHAR(255),
    phone VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS user_status_history (
    id VARCHAR(10) PRIMARY KEY,
    user_id INT,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY user_id REFERENCES users(user_id),
    status VARCHAR(20),
    status_change_decsription VARHCAR(255),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT,

    CONSTRAINT valid_status CHECK (upper(status) in ('ACTIVE', 'INACTIVE' 'DELETED', 'SUSPENDED', 'BANNED'))
);

CREATE TABLE IF NOT EXISTS location
    location_id PRIMARY KEY,
    city VARCHAR(255),
    state VARCHAR(255),
    zip_code VARCHAR(20),
    country VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS user_location (
    id PRIMARY KEY,
    user_id VARCHAR(10),
    FOREIGN KEY user_id REFERENCES users(user_id),
    location_id INT,
    FOREIGN KEY location_id REFERENCES location(location_id),
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current INT
);
