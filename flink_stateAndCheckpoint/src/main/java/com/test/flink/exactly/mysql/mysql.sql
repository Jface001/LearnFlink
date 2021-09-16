
/*
两阶段提交协议
    两阶段提交协议针对Flink的Sink，要求下游的系统支持事务，或者是幂等性。

两阶段提交是指如下两个阶段：
    阶段一：preCommit： 预提交
        在Sink进行snapshot操作的时候调用此方法。
    阶段二：commit： 真正的提交操作
        当系统中各个operator的checkpoint操作都成功之后，JobManager会通知各个operator checkpoint操作已完成。
        此时会调用该方法。

TwoPhaseCommitSinkFunction
    该类是实现两阶段提交Sink的父类，封装了两阶段提交的主要逻辑。
    initializeState方法
        该方法在CheckpointedFunction接口中定义，在集群中执行的时候调用，用于初始化状态后端。
    该方法主要有以下逻辑：
        获取状态存储变量state
        提交所有已经执行过preCommit的事务
        终止所有尚未preCommit的事务
        创建一个新事务
 */

-- 创建数据库
CREATE DATABASE IF NOT EXISTS db_flink;

-- 使用数据库
USE db_flink ;

-- 创建表
CREATE TABLE `db_flink`.`tbl_kafka_message` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `value` varchar(255) NOT NULL,
  `insert_time` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tbl_kafka_message_UN` (`value`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;



-- 插入数据
INSERT INTO db_flink.tbl_kafka_message(value, insert_time) VALUES (?, ?) ;


-- 查询数据
SELECT id, value, insert_time FROM db_flink.tbl_kafka_message;
