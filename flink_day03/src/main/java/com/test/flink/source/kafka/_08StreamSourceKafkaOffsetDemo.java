package com.test.flink.source.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: Jface
 * @Date: 2021/9/7 19:54
 * @Desc: Flink从Kafka消费数据，指定topic名称和反序列化类，从指定偏移量开始消费
 */
public class _08StreamSourceKafkaOffsetDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        // TODO: 1、Flink从topic中最初的数据开始消费
       kafkaConsumer.setStartFromEarliest();
        // TODO: 2、Flink从topic中最新的数据开始消费
        kafkaConsumer.setStartFromLatest();
        // TODO: 3、Flink从topic中指定的group上次消费的位置开始消费，所以必须配置group.id参数
        kafkaConsumer.setStartFromGroupOffsets();
        // TODO: 4、Flink从topic中不同分区时间戳开始
        Map<KafkaTopicPartition,Long> offsets = new HashMap<>();
        offsets.put(new KafkaTopicPartition("flink-topic", 0), 12080L) ;
        offsets.put(new KafkaTopicPartition("flink-topic", 1), 11260L);
        offsets.put(new KafkaTopicPartition("flink-topic", 2), 11240L);
        // TODO: 5、Flink从topic中指定的offset开始，这个比较复杂，需要手动指定offset
        kafkaConsumer.setStartFromSpecificOffsets(offsets);



        // TODO: 5、指定时间戳消费数据
        kafkaConsumer.setStartFromTimestamp(1630982721990L) ;


        // 2-3. 添加数据源
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaConsumer);
        // 3. 数据转换-transformation，不需要转换
        // 4. 数据终端-sink
        kafkaDataStream.printToErr();
        // 5. 触发执行
        env.execute(_08StreamSourceKafkaOffsetDemo.class.getName());


    }
}
