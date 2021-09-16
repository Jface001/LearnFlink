package com.test.flink.exactly.kafka;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;

/**
 *编写Flink Stream应用程序：自定义数据源，模拟生成测试数据到【flink_input_topic】中。
 */
public class StreamKafkaProducerDemo {

	/**
	 * 将数据流DataStream保存到Kafka Topic中，使用Flink Kafka Connector连接器中FlinkKafkaProducer
	 */
	private static void kafkaSink(DataStream<String> stream, String topic){
		// 4-1. 向Kafka写入数据时属性设置
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "node1:9092");
		props.put("group.id", "group_id_20001") ;
		// 4-2. 写入数据时序列化
		KafkaSerializationSchema<String> kafkaSchema = new KafkaSerializationSchema<String>() {
			@Override
			public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
				return new ProducerRecord<byte[], byte[]>(topic, element.getBytes());
			}
		};
		// 4-3. 创建Producer对象
		FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
			topic,
			kafkaSchema,
			props,
			FlinkKafkaProducer.Semantic.EXACTLY_ONCE
		) ;
		// 4-4. 添加Sink
		stream.addSink(producer);
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStream<String> inputStream = env.addSource(new RichParallelSourceFunction<String>() {
			private boolean isRunning = true;
			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				Random random = new Random();
				TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");
				// 循环产生数据
				while (isRunning){
					Instant instant = Instant.ofEpochMilli(
						System.currentTimeMillis() + timeZone.getOffset(System.currentTimeMillis())
					);
					String ouptut = String.format(
						"{\"ts\": \"%s\",\"user_id\": \"%s\", \"item_id\":\"%s\", \"category_id\": \"%s\"}",
						instant.toString(),
						"user_" + (10000 + random.nextInt(10000)),
						"item_" + (100000 + random.nextInt(100000)),
						"category_" + (200 + random.nextInt(200))
					);
					System.out.println(ouptut);
					ctx.collect(ouptut);
					// 每隔1秒产生1条数据
					Thread.sleep(1000);
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}
		}) ;

		// 3. 数据转换-transformation
		//DataStream<String> outputStream = null ;

		// 4. 数据终端-sink
		kafkaSink(inputStream, "flink-input-topic");

		// 5. 触发执行-execute
		env.execute("StreamKafkaProducerDemo") ;
	}
}
