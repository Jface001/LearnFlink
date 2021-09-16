package com.test.flink.asyncio;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @Author: Jface
 * @Date: 2021/9/16 20:16
 * @Desc: 异步请求MySQL数据库，依据userId获取userName，采用线程池方式
 */
public class AsyncMySQLRequest extends RichAsyncFunction<Tuple2<String, String>, String> {

    // 定义变量
    Connection conn = null ;
    PreparedStatement pstmt = null ;
    ResultSet result = null ;

    // 定义一个线程池
    ExecutorService executorService = null ;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化生成一个线程池
        executorService = Executors.newFixedThreadPool(10) ;

        // a. 加载驱动类
        Class.forName("com.mysql.jdbc.Driver") ;
        // b. 获取连接
        conn = DriverManager.getConnection(
                "jdbc:mysql://node1:3306/?useSSL=false",
                "root",
                "123456"
        );
        // c. 构建Statement对象
        pstmt = conn.prepareStatement("SELECT user_name FROM db_flink.tbl_user_info WHERE user_id = ?") ;
    }

    @Override
    public void asyncInvoke(Tuple2<String, String> input, ResultFuture<String> resultFuture) throws Exception {
        // input表示数据流中每条数据： <u_1007 -> u_1007,browser,2021-09-01 16:25:19.793>
        String userId = input.f0 ;

        // 采用线程池方式，异步请求MySQL数据库
        Future<String> future = executorService.submit(
                new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        // 定义变量
                        String userName = "未知" ;

                        // d. 设置插入数据时占位符的值
                        pstmt.setString(1, userId);
                        // e. 查询MySQL数据库
                        result = pstmt.executeQuery();
                        // f. 获取查询的值，
                        if(result.next()){
                            userName = result.getString(1) ;
                        }
                        // 返回数据
                        return userName;
                    }
                }
        );

        // 组合请求结果字符串
        String output = future.get() + "," + input.f1 ;
        // 将异步请求结果返回
        resultFuture.complete(Collections.singletonList(output));
    }

    @Override
    public void timeout(Tuple2<String, String> input, ResultFuture<String> resultFuture) throws Exception {
        // input 表示数据流中每条数据，TODO：如果请求MySQL数据库超时，直接返回：未知或unknown
        String output = "unknown," + input.f1;

        // 返回结果
        resultFuture.complete(Collections.singletonList(output));
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

