package com.test.flink.broadcast;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源，实时从MySQL表获取数据，实现接口RichSourceFunction
 */
public class UserInfoSource extends RichSourceFunction<UserInfo> {

	// 标识符，是否实时接收数据
	private boolean isRunning = true;

	private Connection conn = null;
	private PreparedStatement pstmt = null;
	private ResultSet rs = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		// 1. 加载驱动
		Class.forName("com.mysql.jdbc.Driver");
		// 2. 创建连接
		conn = DriverManager.getConnection(
			"jdbc:mysql://node3:3306/?useUnicode=true&characterEncoding=utf-8",
			"root",
			"123456"
		);
		// 3. 创建PreparedStatement
		pstmt = conn.prepareStatement("select userId, userName, userAge from db_flink.user_info");
	}

	@Override
	public void run(SourceContext<UserInfo> ctx) throws Exception {
		while (isRunning) {
			// 1. 执行查询
			rs = pstmt.executeQuery();
			// 2. 遍历查询结果,收集数据
			while (rs.next()) {
				String id = rs.getString("userId");
				String name = rs.getString("userName");
				Integer age = rs.getInt("userAge");
				UserInfo userInfo = new UserInfo(id, name, age);
				// 输出
				ctx.collect(userInfo);
			}
			// 每隔3秒查询一次
			TimeUnit.SECONDS.sleep(3);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void close() throws Exception {
		if (null != rs) {
			rs.close();
		}
		if (null != pstmt) {
			pstmt.close();
		}
		if (null != conn) {
			conn.close();
		}
	}

}

