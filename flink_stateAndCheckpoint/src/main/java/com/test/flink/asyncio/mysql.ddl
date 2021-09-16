
-- 创建数据库
CREATE DATABASE IF NOT EXISTS db_flink;

-- 使用数据库
USE db_flink ;

-- 创建表
CREATE TABLE IF NOT EXISTS db_flink.tbl_user_info (
    user_id varchar(100) NOT NULL,
    user_name varchar(255) NOT NULL,
    CONSTRAINT tbl_user_info_PK PRIMARY KEY (user_id)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

-- 插入数据
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1000', 'zhenshi') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1001', 'zhangsan') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1002', 'lisi') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1003', 'wangwu') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1004', 'zhaoliu') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1005', 'tianqi') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1006', 'qianliu') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1007', 'sunqi') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1008', 'zhouba') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1009', 'wujiu') ;

