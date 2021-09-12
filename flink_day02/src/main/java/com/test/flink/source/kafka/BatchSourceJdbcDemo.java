package com.test.flink.source.kafka;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * @Author: Jface
 * @Date: 2021/9/5 19:33
 * @Desc: DataSet API 批处理中数据源，从RDBMs数据库表中读取数据
 */
public class BatchSourceJdbcDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.数据源-source
        DataSource<Row> dataSet = env.createInput(JDBCInputFormat
                .buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/?useSSL=false")
                .setUsername("root")
                .setPassword("root")
                .setQuery("select id,name from wzry.heros limit 100")
                //获取字段的类型
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO ))
                .finish()
        );
        //3.输出终端-sink
        dataSet.print();
    }
}
