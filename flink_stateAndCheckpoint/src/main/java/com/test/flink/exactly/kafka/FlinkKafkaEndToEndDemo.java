package com.test.flink.exactly.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Flink Kafka端到端精准一致性测试
 *      从Flink1.4.0版本开始，Kafka版本高于0.11的Kafka Sink可以通过二阶段事务提交构建端到端一致性的实时应用
 *      https://flink.apache.org/features/2018/03/01/end-to-end-exactly-once-apache-flink.html
 */
public class FlinkKafkaEndToEndDemo {

	/**
	 * Flink Stream流式应用，Checkpoint检查点属性设置
	 */
	private static void setEnvCheckpoint(StreamExecutionEnvironment env) {
		// 1. 设置Checkpoint时间间隔
		env.enableCheckpointing(1000);

		// 2. 设置状态后端
		env.setStateBackend(new FsStateBackend("file:///D:/flink-checkpoints/"));

		// 3. 设置两个Checkpoint 之间最少等待时间，
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

		// 4-1. 设置如果在做Checkpoint过程中出现错误，是否让整体任务失败
		env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
		// 4-2. 设置Checkpoint时失败次数，允许失败几次
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

		// 5. 设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint
		env.getCheckpointConfig().enableExternalizedCheckpoints(
			CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);

		// 6. 设置checkpoint的执行模式为EXACTLY_ONCE(默认)，注意：需要外部支持，如Source和Sink的支持
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// 7. 设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
		env.getCheckpointConfig().setCheckpointTimeout(60000);

		// 8. 设置同一时间有多少个checkpoint可以同时执行
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

		// 9. 设置重启策略：NoRestart
		env.setRestartStrategy(RestartStrategies.noRestart());
	}

	/**
	 * 从Kafka实时消费数据，使用Flink Kafka Connector连接器中FlinkKafkaConsumer
	 */
	private static DataStream<String> kafkaSource(StreamExecutionEnvironment env, String topic) {
		// 2-1. 消费Kafka数据时属性设置
		Properties props = new Properties();
		props.put("bootstrap.servers", "node1:9092") ;
		props.put("group.id", "group_id_10001") ;
		props.put("flink.partition-discovery.interval-millis", "10000") ;

		// 2-2. 创建Consumer对象
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
			topic,
			new SimpleStringSchema(),
			props
		) ;
		//消费策略，从最新的位置开始消费
		kafkaConsumer.setStartFromLatest();

		// 2-3. 添加数据源
		return env.addSource(kafkaConsumer);
	}

	/**
	 * 依据业务对DataStream数据流中数据进行转换处理，调用Transformation函数
	 */
	private static DataStream<String> processStream(DataStream<String> stream){
		return stream ;
	}

	/**
	 * 将数据流DataStream保存到Kafka Topic中，使用Flink Kafka Connector连接器中FlinkKafkaProducer
	 */
	private static void kafkaSink(DataStream<String> stream, String topic){
		// 4-1. 向Kafka写入数据时属性设置
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "node1:9092");
		props.put("group.id", "group_id_10002") ;
		// 端到端一致性：需要指定transaction.timeout.ms(默认为1小时)的值，需要小于transaction.max.timeout.ms(默认为15分钟)
		props.setProperty("transaction.timeout.ms", 1000 * 60 * 2 + "");
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
		env.setParallelism(2);
		// TODO: 设置Checkpoint和Restart
		setEnvCheckpoint(env);

		// 2. 数据源-source
		DataStream<String> inputStream = kafkaSource(env, "flink-input-topic") ;

		// 3. 数据转换-transformation
		DataStream<String> outputStream = processStream(inputStream) ;

		// 4. 数据终端-sink
		kafkaSink(outputStream, "flink-output-topic");

		// 5. 触发执行-execute
		env.execute("StreamExactlyOnceKafkaDemo") ;
	}

}
