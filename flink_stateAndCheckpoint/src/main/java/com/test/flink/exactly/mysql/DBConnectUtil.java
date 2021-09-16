package com.test.flink.exactly.mysql;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * 数据库操作工具类，比如获取连接Connection，提交事务和事务回滚等
 */
public class DBConnectUtil {

	/**
	 * 依据MySQL数据库URL、USER和PASSWORD获取连接Connection对象
	 */
	public static Connection getConnection(String url, String user, String password) {
		// 定义变量
		Connection conn = null;
		try{
			// a. 加载驱动类
			Class.forName("com.mysql.cj.jdbc.Driver");
			// b. 获取连接
			conn = DriverManager.getConnection(url, user, password);
			// c.设置手动提交
			conn.setAutoCommit(false);
		}catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * 关闭连接
	 */
	public static void closeConnection(Connection conn) {
		try{
			if(null != conn) {
				conn.close();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * 手动提交事务
	 */
	public static void commitTransaction(Connection conn) {
		try{
			if(null != conn){
				conn.commit();
			}
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			closeConnection(conn);
		}
	}

	/**
	 * 事务回滚
	 */
	public static void rollback(Connection conn) {
		try{
			if(null != conn){
				conn.rollback();
			}
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			closeConnection(conn);
		}
	}

}
