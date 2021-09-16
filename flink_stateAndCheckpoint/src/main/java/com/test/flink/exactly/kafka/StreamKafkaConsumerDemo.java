package com.test.flink.exactly.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 编写Flink Stream应用程序：从Kafka的【flink_output_topic】中实时消费数据，打印纸控制台。
 */
public class StreamKafkaConsumerDemo {

	/**
	 * 从Kafka实时消费数据，使用Flink Kafka Connector连接器中FlinkKafkaConsumer
	 */
	private static DataStream<String> kafkaSource(StreamExecutionEnvironment env, String topic) {
		// 2-1. 消费Kafka数据时属性设置
		Properties props = new Properties();
		props.put("bootstrap.servers", "node1:9092") ;
		props.put("group.id", "group_id_30001") ;
		props.put("flink.partition-discovery.interval-milli", "10000") ;

		// 2-2. 创建Consumer对象
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
			topic,
			new SimpleStringSchema(),
			props
		) ;
		kafkaConsumer.setStartFromLatest();
		// 2-3. 添加数据源
		return env.addSource(kafkaConsumer);
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStream<String> inputStream = kafkaSource(env, "flink-output-topic") ;

		// 3. 数据转换-transformation
		//DataStream<String> outputStream = null ;

		// 4. 数据终端-sink
		inputStream.printToErr();

		// 5. 触发执行-execute
		env.execute("FlinkKafkaConsumerDemo") ;
	}

}
