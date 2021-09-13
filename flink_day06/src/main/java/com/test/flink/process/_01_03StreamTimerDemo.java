package com.test.flink.process;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Jface
 * @Date: 2021/9/13 16:39
 * @Desc: 使用 Flink 计算引擎实现流式数据处理：
 * 从自定义数据源读取订单信息，使用定时器 Timer，下单超过 60 秒还没有付款则取消订单
 */


public class _01_03StreamTimerDemo {

    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        DataStreamSource<String> inputStream = env.addSource(new OrderSource());
        //3.处理数据-transformation，将订单数据存入 MYSQL，如果 1分钟还没有支付，将订单状态改为 “取消”
        // 数据格式：2021091016543115116358,4000005221,2021-09-10 16:54:31.151,未付款,49.34
        SingleOutputStreamOperator<OrderData> processStream = inputStream
                //3.1 按照订单 id 分组，方便使用 KeyedProcessFunction
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String line) throws Exception {
                        return line.split(",")[0];
                    }
                })      //3.2 解析订单数据封装到实体类实例对象
                .process(new KeyedProcessFunction<String, String, OrderData>() {
                    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

                    @Override
                    public void processElement(String value, Context ctx, Collector<OrderData> out) throws Exception {
                        String[] arr = value.split(",");
                        OrderData orderData = new OrderData(arr[0], arr[1], arr[2], arr[3], Double.parseDouble(arr[4]));
                        out.collect(orderData);

                        //3.3 当订单状态为：未付款，设置定时器，在一定时间后，再查询判断是否付款，未付款状态更该为 “ 取消 ”
                        //这里模拟按照处理时间来判断
                        if ("未付款".equals(orderData.getOrderStatus())) {
                            System.out.println("订单[" + orderData.orderId + "]设置定时器，在1分钟后检查是否付款，如果未付款，直接取消订单");
                            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 1*60 * 1000);
                        }
                    }

                    //3.4 注册定时器之后，触发定时器任务
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderData> out) throws Exception {
                        //获取订单 id，未付款订单
                        String orderId = ctx.getCurrentKey();
                        //依据订单 id 获取订单状态
                        String orderStatus = queryStatus(orderId);
                        // 判断订单状态，如果还是 “ 未付款 ” 状态改为 “ 取消 ”，通过自定义工具类实现
                        if ("未付款".equals(orderStatus)) {
                            updateStatus(orderId);
                            System.out.println("更新订单[" + orderId + "]状态：取消...............");
                        }
                    }

                    //依据orderId更新订单状态为：取消
                    private void updateStatus(String orderId) throws Exception {
                        // a. 加载驱动类，获取连接
                        Class.forName("com.mysql.jdbc.Driver");
                        Connection conn = DriverManager.getConnection(
                                "jdbc:mysql://node3:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false",
                                "root", "123456"
                        );
                        // b. 执行更新
                        PreparedStatement pstmt = conn.prepareStatement("UPDATE db_flink.tbl_orders SET order_status = ? WHERE order_id = ?");
                        pstmt.setString(1, "取消");
                        pstmt.setString(2, orderId);
                        pstmt.executeUpdate();
                        // c. 关闭连接
                        pstmt.close();
                        conn.close();
                    }

                    // 依据orderId查询订单状态
                    private String queryStatus(String orderId) throws Exception {
                        // a. 加载驱动类，获取连接
                        Class.forName("com.mysql.jdbc.Driver");
                        Connection conn = DriverManager.getConnection(
                                "jdbc:mysql://node3:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false",
                                "root", "123456"
                        );
                        // b. 执行查询
                        PreparedStatement pstmt = conn.prepareStatement("SELECT order_status FROM db_flink.tbl_orders WHERE order_id = ?");
                        pstmt.setString(1, orderId);
                        ResultSet result = pstmt.executeQuery();
                        // c. 获取订单状态
                        String orderStatus = "unknown";
                        while (result.next()) {
                            orderStatus = result.getString(1);
                        }
                        // d. 关闭连接
                        result.close();
                        pstmt.close();
                        conn.close();
                        // e. 返回
                        return orderStatus;
                    }
                });


        //4.输出结果-sink，保存到 MySQL
        processStream.addSink(new MySQLSink());


        //5.触发执行-execute
        env.execute(_01_03StreamTimerDemo.class.getName());

    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    private static class OrderData {
        private String orderId;
        private String userId;
        private String orderTime;
        private String orderStatus;
        private Double orderAmount;
    }

    /**
     * 自定义数据源Source，实时产生交易订单数据：orderId,userId,orderTime,orderStatus,orderAmount
     */
    private static class OrderSource extends RichParallelSourceFunction<String> {
        String[] allStatus = new String[]{"未付款", "已付款", "已付款", "已付款", "未付款"};
        private boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Random random = new Random();
            FastDateFormat format = FastDateFormat.getInstance("yyyyMMddHHmmssSSS");
            FastDateFormat format2 = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");
            while (isRunning) {
                long currentTimeMillis = System.currentTimeMillis();
                String orderId = format.format(currentTimeMillis) + "" + (10000 + random.nextInt(10000));
                String userId = (random.nextInt(5) + 1) * 100000 + random.nextInt(100000) + "";
                String orderTime = format2.format(currentTimeMillis);
                String orderStatus = allStatus[random.nextInt(allStatus.length)];
                Double orderAmount = new BigDecimal(random.nextDouble() * 100).setScale(2, RoundingMode.HALF_UP).doubleValue();

                // 输出字符串
                String output = orderId + "," + userId + "," + orderTime + "," + orderStatus + "," + orderAmount;
                ctx.collect(output);

                TimeUnit.MILLISECONDS.sleep(2000 + random.nextInt(10000));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 将数据流保存至MySQL数据库表中
     */
    private static class MySQLSink extends RichSinkFunction<OrderData> {
        // 定义变量
        Connection conn = null;
        PreparedStatement pstmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            // a. 加载驱动类
            Class.forName("com.mysql.jdbc.Driver");
            // b. 获取连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://node3:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456"
            );
            // c. 获取PreparedStatement实例
            pstmt = conn.prepareStatement("INSERT INTO db_flink.tbl_orders (order_id, user_id, order_time, order_status, order_amount) VALUES (?,?,?,?,?)");
        }

        @Override
        public void invoke(OrderData order, Context context) throws Exception {
            // d. 设置占位符值
            pstmt.setString(1, order.orderId);
            pstmt.setString(2, order.userId);
            pstmt.setString(3, order.orderTime);
            pstmt.setString(4, order.orderStatus);
            pstmt.setDouble(5, order.orderAmount);
            // e. 执行插入
            pstmt.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (null != pstmt && !pstmt.isClosed()) {
                pstmt.close();
            }
            if (null != conn && !conn.isClosed()) {
                conn.close();
            }
        }
    }
}
