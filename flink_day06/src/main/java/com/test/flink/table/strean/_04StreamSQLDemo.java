package com.test.flink.table.strean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @Author: Jface
 * @Date: 2021/9/14 14:27
 * @Desc: Flink SQL流式数据处理案例演示，官方Example案例。
 */
public class _04StreamSQLDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.5 构建Stream Table 执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 数据源-source, 模拟数据集
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1001L, "beer", 3),
                new Order(1001L, "diaper", 4),
                new Order(1003L, "rubber", 2)
        ));
        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(1002L, "pen", 3),
                new Order(1002L, "rubber", 3),
                new Order(1004L, "beer", 1)
        ));

        //3.处理数据-transformation
        //3.1 将DataStream 数据转化为 Table 并注册临时视图
        Table tableA = tableEnv.fromDataStream(orderA, "user, product, amount");
        tableEnv.createTemporaryView("orderB", orderB, "user, product, amount");
        //3.2 使用 SQL查询数据，分别对 2 个表做数据查询，将结果合并
        Table resultTable = tableEnv.sqlQuery(
                "select * from " + tableA + " where amount > 2 union all select * from orderB where amount > 2");
        //3.3 将 Table 转化为 DataStream
        DataStream<Order> orderDataStream = tableEnv.toAppendStream(resultTable, Order.class);
        //4.输出结果-sink
        orderDataStream.printToErr();

        //5.触发执行-execute
        env.execute(_04StreamSQLDemo.class.getName());

    }

    // 定义JavaBean类，封装数据
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public Integer amount;
    }
}
