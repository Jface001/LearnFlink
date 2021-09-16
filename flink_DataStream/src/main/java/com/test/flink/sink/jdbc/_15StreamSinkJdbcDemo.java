package com.test.flink.sink.jdbc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 案例演示：使用JDBC Sink 连接器，将数据保存至MySQL表中，继承RichSinkFunction
 */
public class _15StreamSinkJdbcDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Student> inputDataStream = env.fromElements(
			new Student(23, "wangwu2", 20),
			new Student(24, "zhaoliu2", 19),
			new Student(25, "wangwu2", 20),
			new Student(26, "zhaoliu2", 19)
		);

		// 3. 数据转换-transformation
		// 4. 数据终端-sink
		// 4-1. 构建Sink对象，设置属性
		SinkFunction<Student> jdbcSink = JdbcSink.sink(
			"REPLACE INTO db_flink.t_student (id, name, age) VALUES (?, ?, ?)", //
			new JdbcStatementBuilder<Student>() {
				@Override
				public void accept(PreparedStatement pstmt, Student student) throws SQLException {
					pstmt.setInt(1, student.id);
					pstmt.setString(2, student.name);
					pstmt.setInt(3, student.age);
				}
			},//
			// TODO: 设置批量写入时，参数值，尤其是批量大小，在实时写入处理时，设置为1
			new JdbcExecutionOptions.Builder()
				.withBatchSize(1) // 默认为5000
				.withBatchIntervalMs(0)
				.withMaxRetries(3)
				.build(),//
			new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withDriverName("com.mysql.jdbc.Driver")
				.withUrl("jdbc:mysql://node1:3306/?useSSL=false")
				.withUsername("root")
				.withPassword("123456")
				.build()
		);
		// 4-2. DataStream数据流添加Sink
		inputDataStream.addSink(jdbcSink) ;

		// 5. 触发执行-execute
		env.execute(_15StreamSinkJdbcDemo.class.getSimpleName()) ;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class Student{
		private Integer id ;
		private String name ;
		private Integer age ;
	}
}
