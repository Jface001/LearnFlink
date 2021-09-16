package com.test.flink.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
/**
 * @Author: Jface
 * @Date: 2021/9/14 22:46
 * @Desc:
 * 官方案例演示：滑动窗口JOIN，基于事件时间EventTime
 *      窗口大小window size：2 秒，滑动大小slide size：1 秒
 */
public class _08_02SlidingWindowJoinDemo {
    public static void main(String[] args) throws Exception{
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO: 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 数据源-source
        // a. 第1条流Stream -> node1:9999
        DataStreamSource<String> dataStream01 = env.socketTextStream("node1", 9999);

        // b. 第2条流Stream -> node1:8888
        DataStreamSource<String> dataStream02 = env.socketTextStream("node1", 8888);

/*
10001,0,k1,green
11001,1,k1,green
---
10001,0,k1,orange
11001,1,k1,orange


13001,3,k2,green
---
12001,2,k2,orange
13001,3,k2,orange


14001,4,k3,green
15001,5,k3,green
---
15001,5,k3,orange


16001,6,k44,green
---
16001,6,k444,orange
*/

        // 3. 数据转换-transformation
        // a. green DataStream转换
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> greenDataStream = dataStream01
                .filter(line -> null != line && line.trim().split(",").length == 4)
                // 指定事件时间字段EventTime，必须为Long类型
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.trim().split(",")[0]);
                    }
                })
                // 解析数据，封装元组中
                .map(new MapFunction<String, Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> map(String line) throws Exception {
                        // 分割数据
                        String[] array = line.trim().split(",");
                        String key = array[2];
                        Integer number = Integer.parseInt(array[1]);
                        String type = array[3];
                        return Tuple3.of(key, number, type);
                    }
                });

        // b. orange DataStream转换
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> orangeDataStream = dataStream02
                .filter(line -> null != line && line.trim().split(",").length == 4)
                // 指定事件时间字段EventTime，必须为Long类型
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        return Long.parseLong(line.trim().split(",")[0]);
                    }
                })
                // 解析数据，封装元组中
                .map(new MapFunction<String, Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> map(String line) throws Exception {
                        // 分割数据
                        String[] array = line.trim().split(",");
                        String key = array[2];
                        Integer number = Integer.parseInt(array[1]);
                        String type = array[3];
                        return Tuple3.of(key, number, type);
                    }
                });

        // TODO：2个Stream JOIN操作
        DataStream<String> joinDataStream = greenDataStream
                // 第一步、join 数据流
                .join(orangeDataStream)
                // 第二步、指定条件
                .where(tuple -> tuple.f0)
                .equalTo(tuple -> tuple.f0)
                // 第三步、window窗口
                .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
                // 第四步、窗口数据JOIN处理
                .apply(new JoinFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>, String>() {
                    @Override
                    public String join(Tuple3<String, Integer, String> left,
                                       Tuple3<String, Integer, String> right) throws Exception {
                        // left datastream: green
                        String leftValue = left.f2 + "-" + left.f1 ;
                        // right datastream: orange
                        String rightValue = right.f2 + "-" + right.f1 ;
                        // 返回关联数据
                        return left.f0 + " -> " + leftValue + ", " + rightValue;
                    }
                });


        // 4. 数据终端-sink
        joinDataStream.printToErr();

        // 5. 触发执行-execute
        env.execute(_08_02SlidingWindowJoinDemo.class.getSimpleName()) ;


    }
}
