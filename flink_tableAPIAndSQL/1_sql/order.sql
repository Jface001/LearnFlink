
CREATE DATABASE IF NOT EXISTS db_flink ;

USE db_flink;

CREATE TABLE db_flink.tbl_orders (
                                     order_id varchar(255) NOT NULL,
                                     user_id varchar(100) NOT NULL,
                                     order_time varchar(255) NOT NULL,
                                     order_status varchar(100) NOT NULL,
                                     order_amount DOUBLE NOT NULL,
                                     CONSTRAINT tbl__orders_PK PRIMARY KEY (order_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;


-- 插入语句
INSERT INTO db_flink.tbl_orders (order_id, user_id, order_time, order_status, order_amount) VALUES (?,?,?,?,?) ;

-- 查询语句
SELECT order_status FROM db_flink.tbl_orders WHERE order_id = ? ;

-- 更新语句
UPDATE db_flink.tbl_orders SET order_status = ? WHERE order_id = ? ;