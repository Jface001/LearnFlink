package com.test.flink.exactly.mysql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 实现Flink提供2PC接口，将数据流DataStream保存到MySQL数据库表中。
 */
public class FlinkMySQLEndToEndDemo {

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
		props.put("group.id", "group_id_90001") ;
		// 如果有记录偏移量从记录的位置开始消费,如果没有从最新的数据开始消费
		props.put("auto.offset.reset", "latest");
		// 开一个后台线程每隔10s检查Kafka的分区状态
		props.put("flink.partition-discovery.interval-millis", "10000") ;

		// 2-2. 创建Consumer对象
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
			topic,
			new SimpleStringSchema(),
			props
		) ;
		// 从group offset记录的位置位置开始消费,如果kafka broker 端没有该group信息，会根据"auto.offset.reset"的设置来决定从哪开始消费
		kafkaConsumer.setStartFromGroupOffsets();
		// Flink 执 行 Checkpoint 的 时 候 提 交 偏 移 量 (一份在Checkpoint中,一份在Kafka的默认主题中__comsumer_offsets(方便外部监控工具去看))
		kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

		// 2-3. 添加数据源
		return env.addSource(kafkaConsumer).name("kafkaSource");
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration()) ;
		env.setParallelism(1);
		// 设置检查点Checkpoint
		setEnvCheckpoint(env);

		// 2. 数据源-source
		DataStream<String> inputStream = kafkaSource(env, "mysql-topic") ;
		//inputStream.printToErr();

		// 3. 数据转换-transformation
		// 4. 数据终端-sink
		inputStream.addSink(new MySQLTwoPhaseCommitSink()).name("MySQL2PCSink") ;

		// 5. 触发执行-execute
		env.execute("FlinkMySQLEndToEnd") ;
	}

}
