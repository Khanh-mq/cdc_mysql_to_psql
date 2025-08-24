-- Khởi tạo database và bảng cho MySQL
CREATE DATABASE IF NOT EXISTS source_db;
USE source_db;

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nyc_taxi (
    VendorID INT COMMENT 'Vendor ID (1 = Creative Mobile Technologies, LLC; 2 = VeriFone Inc.)',
    tpep_pickup_datetime DATETIME COMMENT 'Pickup datetime',
    tpep_dropoff_datetime DATETIME COMMENT 'Dropoff datetime',
    passenger_count FLOAT COMMENT 'Number of passengers',
    trip_distance FLOAT COMMENT 'Trip distance in miles',
    RatecodeID FLOAT COMMENT 'Rate code ID',
    store_and_fwd_flag VARCHAR(1) COMMENT 'Store and forward flag',
    PULocationID INT COMMENT 'Pickup location ID',
    DOLocationID INT COMMENT 'Dropoff location ID',
    payment_type INT COMMENT 'Payment type',
    fare_amount FLOAT COMMENT 'Fare amount',
    extra FLOAT COMMENT 'Extra amount',
    mta_tax FLOAT COMMENT 'MTA tax',
    tip_amount FLOAT COMMENT 'Tip amount',
    tolls_amount FLOAT COMMENT 'Tolls amount',
    improvement_surcharge FLOAT COMMENT 'Improvement surcharge',
    total_amount FLOAT COMMENT 'Total amount',
    congestion_surcharge FLOAT COMMENT 'Congestion surcharge',
    Airport_fee FLOAT COMMENT 'Airport fee'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo user và phân quyền
CREATE USER IF NOT EXISTS 'mysqluser'@'%' IDENTIFIED BY 'mysqlpass';
GRANT ALL PRIVILEGES ON source_db.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;

-- Thủ tục thêm nhiều users nhanh
DELIMITER //
CREATE PROCEDURE add_user(IN i INT)
BEGIN
    WHILE i > 0 DO 
        INSERT INTO users(name, email) 
        VALUES (CONCAT('user', i), CONCAT('email', i, '@example.com'));
        SET i = i - 1;
    END WHILE;
END //
DELIMITER ;
