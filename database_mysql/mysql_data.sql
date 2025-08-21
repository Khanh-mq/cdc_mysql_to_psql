-- Khởi tạo database và bảng cho MySQL
CREATE DATABASE IF NOT EXISTS source_db;
USE source_db;

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- tao một cái procedure để chèn user vào nhanh hơn COMMENT
DELIMITER //
CREATE PROCEDURE add_user(in i int )
BEGIN
    while i  > 0 do 
        insert into users(name , email) values (concat('user' , i ) , concat('email' , i));
        set i = i -  1;
        end while;
END //
DELIMITER ;