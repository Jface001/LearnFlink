package com.test.flink.source.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

/**
 * @Author: Jface
 * @Date: 2021/9/7 19:54
 * @Desc: Flink从Kafka消费数据，指定topic名称和反序列化类
 */
public class _07StreamSourceKafkaDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 数据源-source
        // 2-1. 设置消费Kafka数据时属性参数值
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092,node2:9092，node3:9092");
        properties.setProperty("group.id", "test-1001");
        // 2-2. 构建Consumer实例对象
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "flink-topic",
                new SimpleStringSchema(),
                properties);
        // 2-3. 添加数据源
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaConsumer);
        // 3. 数据转换-transformation，不需要转换
        // 4. 数据终端-sink
        kafkaDataStream.printToErr();
        // 5. 触发执行
        env.execute(_07StreamSourceKafkaDemo.class.getName());


    }
}
