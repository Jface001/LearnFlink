package com.test.flink.source.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Jface
 * @Date: 2021/9/7 19:15
 * @Desc: 从MySQL中实时加载数据：要求MySQL中的数据有变化，也能被实时加载出来
 */
public class _06StreamSourceMySQLDemo {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static class MysqlSource extends RichParallelSourceFunction<Student> {
        private boolean running = true;
        Connection conn = null;
        PreparedStatement stat = null;
        ResultSet rs = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //1.加载驱动类
            Class.forName("com.mysql.jdbc.Driver");

            //2.获取连接
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/?useSSL=false"
                    , "root", "root");
            //3.构建 Statement 对象
            stat = conn.prepareStatement("select * from db_flink.t_student ");
        }

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (running) {
                //4.查询数据库
                rs = stat.executeQuery();
                //遍历解析结果集
                while (rs.next()) {
                    int id = rs.getInt(1);
                    String name = rs.getString(2);
                    int age = rs.getInt(3);
                    Student student = new Student(id, name, age);
                    ctx.collect(student);
                }
                //每隔2秒查询一次
                TimeUnit.SECONDS.sleep(2);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void close() throws Exception {
            if (rs != null && !rs.isClosed()) {
                rs.close();
            }
            if (stat != null && !stat.isClosed()) {
                stat.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source，添加自定义的 MySQL 数据源
        DataStreamSource<Student> studentDataStream = env.addSource(new MysqlSource());

        //3.处理数据-transformation，不处理

        //4.输出结果-sink，控制台
        studentDataStream.printToErr();

        //5.触发执行-execute，流处理需要触发
        env.execute(_06StreamSourceMySQLDemo.class.getName());
    }
}

