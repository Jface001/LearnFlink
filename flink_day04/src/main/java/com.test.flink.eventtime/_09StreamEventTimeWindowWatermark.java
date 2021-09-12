package com.test.flink.eventtime;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/8 20:26
 * @Desc: 窗口统计案例演示：事件时间滚动窗口（EventTim Tumbling e Window），窗口内数据进行词频统计
 */
public class _09StreamEventTimeWindowWatermark {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO: 0、设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1", 9999);

        // 3. 数据转换-transformation
        //TODO: 1、过滤脏数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDataStream = inputStream.filter(line -> null != line && line.trim().split(",").length == 3)
                //TODO: 2、设置事件时间字，必须为Long类型，考虑乱序的情况，允许乱序的最长时间为2秒
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(String element) {
                        String[] arr = element.trim().split(",");
                        return Long.parseLong(arr[0]);
                    }
                })
                //TODO: 3、切割并封装到二元组
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] arr = value.trim().split(",");
                        String name = arr[1];
                        int counter = Integer.parseInt(arr[2]);
                        // 打印每条业务数据，仅仅将日期时间转换
                        System.out.println(format.format(Long.parseLong(arr[0])) + "," + name + "," + counter);
                        return Tuple2.of(name, counter);
                    }
                });
        SingleOutputStreamOperator<String> resultDataStream = tupleDataStream
                //TODO:3、1 按照卡口分组
                .keyBy(0)
                //TODO:3、2 设置窗口
                .timeWindow(Time.seconds(5))
                //TODO:3、3 设置窗口数据聚合，编码实现如何计算窗口数据
                .apply(new WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
                    private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
                        //a. 获取 key 值, tuple 里面就是 Key，需要转换成一元组
                        String key = ((Tuple1<String>) tuple).f0;
                        //b. 获取时间窗口大小，窗口开始时间和结束时间，转化成指定格式
                        long start = window.getStart();
                        long end = window.getEnd();
                        String startTime = this.format.format(start);
                        String endTime = this.format.format(end);
                        //c. 对窗口中数据聚合操作
                        int total = 0;
                        for (Tuple2<String, Integer> item : input) {
                            total += item.f1;
                        }
                        //d. 拼接字符串，输出数据
                        String output = "[" + startTime + "~" + endTime + "]-->" + key + "=" + total;
                        out.collect(output);
                    }
                });

        // 4. 数据终端-sink
        resultDataStream.printToErr();

        // 5. 触发执行-execute
        env.execute(_09StreamEventTimeWindowWatermark.class.getName());


    }
}
