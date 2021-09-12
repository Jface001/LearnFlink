
-- 创建数据库
CREATE DATABASE IF NOT EXISTS db_flink ;

-- 使用数据库
USE db_flink ;

-- 创建表：全国
CREATE TABLE IF NOT EXISTS db_flink.tbl_report_global (
    global varchar(100) NOT NULL,
    amount DOUBLE NOT NULL,
    PRIMARY KEY (global)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

-- 创建表：各省份
CREATE TABLE IF NOT EXISTS db_flink.tbl_report_province (
    province varchar(100) NOT NULL,
    amount DOUBLE NOT NULL,
    PRIMARY KEY (province)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

-- 创建表：重点城市
CREATE TABLE IF NOT EXISTS db_flink.tbl_report_city (
    city varchar(100) NOT NULL,
    amount DOUBLE NOT NULL,
    PRIMARY KEY (city)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

-- 插入更新语句测试
REPLACE INTO db_flink.tbl_report_global(global, amount) VALUES ('上海',999.90) ;
-- 插入语句测试
INSERT INTO db_flink.tbl_report_global(global, amount) VALUES (?, ?) ;


-- ===============================================================
-- 创建表：全国
CREATE TABLE IF NOT EXISTS db_flink.tbl_report_daily_global (
    window_start varchar(100) NOT NULL,
    window_end varchar(100) NOT NULL,
    global varchar(100) NOT NULL,
    amount DOUBLE NOT NULL,
    PRIMARY KEY (global, window_start, window_end)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;