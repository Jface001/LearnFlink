package com.test.flink.exactly.mysql;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;

/**
 * 基于2PC接口TwoPhaseCommitSinkFunction类，实现 MySQL 关系型数据库的二阶提交。
 */
public class MySQLTwoPhaseCommitSink
		extends TwoPhaseCommitSinkFunction<String, MySQLTwoPhaseCommitSink.ConnectionState, Void> {
	// 无参构造方法
	public MySQLTwoPhaseCommitSink(){
		super(new KryoSerializer<>(ConnectionState.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
		System.out.println("MySQLTwoPhaseCommitSink...................");
	}

	// 获取连接，开启手动提交事务（getConnection方法中）
	@Override
	public ConnectionState beginTransaction() throws Exception {
		// 获取连接
		Connection conn = DBConnectUtil.getConnection(
			"jdbc:mysql://node1:3306/?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true",
			"root",
			"123456"
		);
		System.out.println("Connection: " + conn + "...................");
		// 返回连接
		return new ConnectionState(conn);
	}

	// 执行数据库入库操作，task初始化的时候调用
	@Override
	public void invoke(ConnectionState transaction, String value, Context context) throws Exception {
		System.out.println("invoke 方法被调用，处理数据：" + value + "...................");
		// a. 构建插入数据SQL语句
		String insertSQL = "REPLACE INTO db_flink.tbl_kafka_message(id, value, insert_time) VALUES (null, ?, ?)" ;
		// b. 创建PreparedStatement实例对象
		PreparedStatement pstmt = transaction.connection.prepareStatement(insertSQL) ;
		// c. 设置值
		pstmt.setString(1, value);
		String currentDate = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
		pstmt.setString(2, currentDate);
		System.out.println("构建SQL语句：" + pstmt.toString());
		// d. 执行插入语句
		pstmt.executeUpdate();
	}

	// 预提交，此处预提交的逻辑在invoke方法中
	@Override
	public void preCommit(ConnectionState transaction) throws Exception {
		System.out.println("preCommit.........................");
	}

	//  如果invoke方法执行正常，则提交事务
	@Override
	public void commit(ConnectionState transaction) {
		System.out.println("commit.........................");
		DBConnectUtil.commitTransaction(transaction.connection);
	}

	// 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
	@Override
	public void abort(ConnectionState transaction) {
		System.out.println("abort.........................");
		DBConnectUtil.rollback(transaction.connection);
	}


	public static class ConnectionState{
		// 定义变量，数据库连接Connection
		private final transient Connection connection ;
		// 构造方法
		public ConnectionState(Connection connection){
			this.connection = connection ;
		}
	}

}

