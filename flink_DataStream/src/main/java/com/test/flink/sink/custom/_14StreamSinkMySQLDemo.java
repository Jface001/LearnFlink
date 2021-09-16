package com.test.flink.sink.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 案例演示：自定义Sink，将数据保存至MySQL表中，继承RichSinkFunction
 */
public class _14StreamSinkMySQLDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Student> inputDataStream = env.fromElements(
			new Student(13, "wangwu1", 20),
			new Student(14, "zhaoliu1", 19),
			new Student(15, "wangwu1", 20),
			new Student(16, "zhaoliu", 29)
		);

		// 3. 数据转换-transformation
		// 4. 数据终端-sink
		inputDataStream.addSink(new MySQLSink()) ;

		// 5. 触发执行-execute
		env.execute(_14StreamSinkMySQLDemo.class.getSimpleName()) ;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class Student{
		private Integer id ;
		private String name ;
		private Integer age ;
	}

	/**
	 * 自定义Sink，将数据保存至MySQL表中
	 */
	private static class MySQLSink extends RichSinkFunction<Student> {

		// 定义变量，三个变
		Connection conn = null ;
		PreparedStatement pstmt = null ;
		ResultSet result = null ;

		@Override
		public void open(Configuration parameters) throws Exception {
			// a. 加载驱动类
			Class.forName("com.mysql.jdbc.Driver") ;
			// b. 获取连接
			conn = DriverManager.getConnection(
				"jdbc:mysql://node1:3306/?useSSL=false",
				"root",
				"123456"
			);
			// c. 构建Statement对象
			pstmt = conn.prepareStatement("INSERT INTO db_flink.t_student(id, name, age) VALUES (?, ?, ?)") ;
			//pstmt = conn.prepareStatement("REPLACE INTO db_flink.t_student (id, name, age) VALUES (?, ?, ?)") ;
			//pstmt = conn.prepareStatement("INSERT INTO db_flink.t_student(id, name, age) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name=?, age=?") ;
		}

		// 数据流DataStream每条数据如何处理，比如写入MySQL数据库
		@Override
		public void invoke(Student student, Context context) throws Exception {
			// d. 设置占位符的值
			pstmt.setInt(1, student.id);
			pstmt.setString(2, student.name);
			pstmt.setInt(3, student.age);
			pstmt.setString(4, student.name);
			pstmt.setInt(5, student.age);
			// e. 执行插入操作
			pstmt.executeUpdate();
		}

		@Override
		public void close() throws Exception {
			if(null != result && !result.isClosed()) {
				result.close();
			}
			if(null != pstmt && !pstmt.isClosed()) {
				pstmt.close();
			}
			if(null != conn && !conn.isClosed()) {
				conn.close();
			}
		}
	}

}