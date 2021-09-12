package com.test.flink.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.types.Row;

import java.sql.Types;

/**
 * @Author: Jface
 * @Date: 2021/9/5 20:06
 * @Desc: DataSet API 批处理中数据终端，保存数据到RDBMs表中，必须为 Row 形式才能保存
 */
public class BatchSinkJdbcDemo {
    public static void main(String[] args) throws Exception {
        //1.执行环境 env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.数据源-source
        DataSource<String> dataSet = env.fromElements("flink", "spark", "presto");
        //3.数据终端-sink,保存到 MySQL 中
        //TODO: 封装数据到 Row 中
        MapOperator<String, Row> resultDataset = dataSet.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                return Row.of(value);
            }
        });
        //TODO: 保存数据到数据库表中，使用JDBCOutputFormat，数据类型，必须是Row
        resultDataset.output(
                JDBCOutputFormat.buildJDBCOutputFormat() //这里是output 牢记牢记
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://node3:3306/?useSSL=false")
                        .setUsername("root")
                        .setPassword("123456")
                        .setQuery("INSERT  INTO db_flink.tbl_word(id, word) VALUES (null, ?)")
                        .setSqlTypes(new int[]{Types.VARCHAR})
                        .finish()
        );
        //4.触发执行，有写出
        env.execute(BatchSinkJdbcDemo.class.getName());
    }
}
