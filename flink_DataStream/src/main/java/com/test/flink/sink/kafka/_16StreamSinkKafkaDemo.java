package com.test.flink.sink.kafka;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 案例演示：将数据保存至Kafka Topic中，直接使用官方提供Connector
 * /export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic flink-topic
 */
public class _16StreamSinkKafkaDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Order> orderDataStream = env.addSource(new OrderSource());

        // 3. 数据转换-transformation：将Order订单对象转换JSON字符串
        SingleOutputStreamOperator<String> jsonDataStream = orderDataStream.map(new MapFunction<Order, String>() {
            @Override
            public String map(Order order) throws Exception {
                return JSON.toJSONString(order);
            }
        });

        // 4. 数据终端-sink
        // 4-1. 向Kafka写入数据属性配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        // 4-2. 构建Producer对象
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "flink-topic",                  // target topic
                new KafkaSchema("flink-topic"),    // serialization schema
                props,                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE // fault-tolerance
        );
        // 4-3. 数据流DataStream添加Sink对象
        jsonDataStream.addSink(kafkaProducer);

        // 5. 触发执行-execute
        env.execute(_16StreamSinkKafkaDemo.class.getSimpleName());
    }

    /**
     * 自定义实现数据序列化，将String字符串转换为字节数组
     */
    private static class KafkaSchema implements KafkaSerializationSchema<String> {
        private String topic;

        public KafkaSchema(String topic) {
            this.topic = topic;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            return new ProducerRecord<byte[], byte[]>(topic, element.getBytes());
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String id;
        private Integer userId;
        private Double money;
        private Long orderTime;
    }

    /**
     * 自定义数据源：每隔1秒产生1条交易订单数据
     */
    private static class OrderSource extends RichParallelSourceFunction<Order> {
        // 定义标识变量，表示是否产生数据
        private boolean isRunning = true;

        // 模拟产生交易订单数据
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                // 构建交易订单数据
                Order order = new Order(
                        UUID.randomUUID().toString().substring(0, 18), //
                        random.nextInt(2) + 1, //
                        random.nextDouble() * 100,//
                        System.currentTimeMillis()
                );

                // 将数据输出
                ctx.collect(order);

                // 每隔1秒产生1条数据，线程休眠
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
