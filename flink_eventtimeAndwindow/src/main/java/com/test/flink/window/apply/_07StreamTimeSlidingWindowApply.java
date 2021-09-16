package com.test.flink.window.apply;

import com.test.flink.window.time._03StreamTimeSlidingWindow;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/8 19:12
 * @Desc: 窗口统计案例演示：时间滑动窗口（Time Sliding  Window），实时交通卡口车流量统计,通过 apply 方法实现
 */
public class _07StreamTimeSlidingWindowApply {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1", 9999);

        // 3. 数据转换-transformation
        //TODO: 1、过滤脏数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDataStream = inputStream.filter(line -> null != line && line.trim().split(",").length == 2)
                //TODO: 2、切割并封装到二元组
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] arr = value.trim().split(",");
                        String name = arr[0];
                        int counter = Integer.parseInt(arr[1]);
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


        //4.输出结果-sink
        resultDataStream.printToErr();

        //5.触发执行-execute
        env.execute(_07StreamTimeSlidingWindowApply.class.getName());


    }
}
