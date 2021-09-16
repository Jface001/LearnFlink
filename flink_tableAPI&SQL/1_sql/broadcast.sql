-- 在MySQL数据库中创建Database和Table

CREATE DATABASE IF NOT EXISTS db_flink ;

USE db_flink ;

DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `user_info` (
                             `userID` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
                             `userName` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                             `userAge` int(11) NULL DEFAULT NULL,
                             PRIMARY KEY (`userID`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;


INSERT INTO `user_info` VALUES ('user_1', '张三', 10);
INSERT INTO `user_info` VALUES ('user_2', '李四', 20);
INSERT INTO `user_info` VALUES ('user_3', '王五', 30);
INSERT INTO `user_info` VALUES ('user_4', '赵六', 40);