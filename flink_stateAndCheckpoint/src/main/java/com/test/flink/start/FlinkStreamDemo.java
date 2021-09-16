package com.test.flink.start;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Flink Stream 流式应用程序编程模板Template
 */
public class FlinkStreamDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		// 2. 数据源-source
		DataStream<String> inputStream = null ;

		// 3. 数据转换-transformation
		DataStream<String> outputStream = null ;

		// 4. 数据终端-sink
		outputStream.printToErr();

		// 5. 触发执行-execute
		env.execute("StreamExactlyOnceKafkaDemo") ;
	}

}
