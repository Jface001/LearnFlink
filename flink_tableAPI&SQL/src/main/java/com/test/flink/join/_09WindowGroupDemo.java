package com.test.flink.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/14 22:48
 * @Desc: 官方案例演示：滚动窗口JOIN，基于事件时间EventTime
 */
public class _09WindowGroupDemo {
    public static void main(String[] args)  throws Exception{
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO: 设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 数据源-source
        // a. 第1条流Stream -> node1.itcast.cn:9999
        DataStreamSource<String> dataStream01 = env.socketTextStream("node1.itcast.cn", 9999);
        // b. 第2条流Stream -> node1.itcast.cn:8888
        DataStreamSource<String> dataStream02 = env.socketTextStream("node1.itcast.cn", 8888);

		/*
		10001,oid-1,main
---
10002,oid-1,detail,oid-1-did-1
10002,oid-1,detail,oid-1-did-2


13001,oid-2,main
---
13001,oid-2,detail,oid-2-did-1


14001,oid-3,main
---
14002,oid-3,detail,oid-3-did-1
14002,oid-3,detail,oid-3-did-2
14002,oid-3,detail,oid-3-did-3


16011,oid-4,main
---
16011,oid-4,detail,oid-4-did-1
		 */
        // 3. 数据转换-transformation
        // a. 主订单 DataStream转换，数据样本：10001,oid-1,main
        SingleOutputStreamOperator<Tuple2<String, String>> mainDataStream = dataStream01
                .filter(line -> null != line && line.trim().split(",").length == 3)
                // TODO：设置事件时间字段，必须为Long类型
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.trim().split(",")[0]);
                    }
                })
                // 数据转换封装到元组
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String line) throws Exception {
                        // 分割单词
                        String[] array = line.trim().split(",");
                        // 订单ID
                        String orderId = array[1];
                        String orderType = array[2];
                        // 返回元组
                        return Tuple2.of(orderId, orderType);
                    }
                });

        // b. 详情订单 DataStream转换，数据格式：10002,oid-1,detail,oid-1-did-1
        SingleOutputStreamOperator<Tuple2<String, String>> detailDataStream = dataStream02
                .filter(line -> null != line && line.trim().split(",").length == 4)
                // TODO：设置事件时间字段，必须为Long类型
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.trim().split(",")[0]);
                    }
                })
                // 数据转换封装到元组
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String line) throws Exception {
                        // 分割单词
                        String[] array = line.trim().split(",");
                        // 订单ID
                        String orderId = array[1];
                        String orderType = array[2];
                        String detailId = array[3] ;
                        // 返回元组
                        return Tuple2.of(orderId, orderType + "-" + detailId);
                    }
                });

        // TODO：2个Stream CoGroup操作
        DataStream<String> resultDataStream = mainDataStream
                // 第一步、cogroup操作
                .coGroup(detailDataStream)
                // 第二步、指定条件
                .where(tuple -> tuple.f0).equalTo(tuple -> tuple.f0)
                // 第三步、指定窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                // 第四步、窗口内数据关联
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> left,
                                        Iterable<Tuple2<String, String>> right,
                                        Collector<String> out) throws Exception {
                        // 如果以左右表（左边流）为准，循环遍历左表
                        for (Tuple2<String, String> first : left) {
                            // 定义变量，标识右表中是否有数据
                            boolean isJoin = false;
                            // TODO: 直接遍历右表数据，当前仅当右表有数据时，才执行遍历
                            for (Tuple2<String, String> second : right) {
                                isJoin = true ;
                                out.collect(first.f0 + " -> " + first.f1 + ", " + second.f1);
                            }

                            // 表示左表有数据，右表没数据，也就是没有join上
                            if(!isJoin){
                                out.collect(first.f0 + " -> " + first.f1 + ", null");
                            }
                        }
                    }
                });

        // 4. 数据终端-sink
        resultDataStream.printToErr();

        // 5. 触发执行-execute
        env.execute(_09WindowGroupDemo.class.getSimpleName());

    }
}
