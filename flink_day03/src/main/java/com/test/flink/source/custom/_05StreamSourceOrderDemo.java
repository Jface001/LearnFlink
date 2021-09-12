package com.test.flink.source.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Jface
 * @Date: 2021/9/7 18:11
 * @Desc: 自定义数据源：每隔1秒产生1条交易订单数据
 */

public class _05StreamSourceOrderDemo {
    /**
     * 每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
     * - 随机生成订单ID：UUID
     * - 随机生成用户ID：0-2
     * - 随机生成订单金额：0-100
     * - 时间戳为当前系统时间：current_timestamp
     * <p>
     * 1、SourceFunction：
     * 非并行数据源(并行度parallelism=1)
     * 2、RichSourceFunction：
     * 多功能非并行数据源(并行度parallelism=1)
     * 3、ParallelSourceFunction：
     * 并行数据源(并行度parallelism>=1)
     * 4、RichParallelSourceFunction：
     * 多功能并行数据源(parallelism>=1)，Kafka数据源使用该接口
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String id;
        private Integer userId;
        private Double money;
        private Long orderTime;
    }

    /**
     * 自定义数据源，继承抽象类：RichParallelSourceFunction，并行的和富有的
     */
    public static class OrderSource extends RichParallelSourceFunction<Order> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            while (running) {
                //创建 Order 对象
                String id = UUID.randomUUID().toString();
                int userId = new Random().nextInt(2) + 1;
                double orderMoney = new Random().nextDouble() * 100;
                long orderTime = System.currentTimeMillis();
                Order order = new Order(id, userId, orderMoney, orderTime);
                //写出结果
                ctx.collect(order);
                //每个一秒产生一条数据
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;

        }
    }

    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source, 添加自定义数据源类型
        DataStreamSource<Order> orderDataStream = env.addSource(new OrderSource());
        //3.处理数据-transformation，不需要
        //4.输出结果-sink，控制台
        orderDataStream.printToErr();
        //5.触发执行-execute，流式计算需要触发执行
        env.execute(_05StreamSourceOrderDemo.class.getName());

    }

}


