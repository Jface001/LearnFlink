package com.test.flink.window.session;

import com.test.flink.window.count._05StreamCountSlidingWindow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author: Jface
 * @Date: 2021/9/8 17:56
 * @Desc: 窗口统计案例演示：时间会话窗口（Time Session Window)，数字累加求和统计
 */
public class _06StreamSessionWindow {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1", 9999);

        //3.处理数据-transformation
        SingleOutputStreamOperator<Tuple1<Integer>> mapDataStream = inputDataStream
                //TODO: 1.过滤脏数据
                .filter(line -> null != line && line.trim().length() > 0)
                //TODO: 2.提取字段，转化成 Integer 类型,使用一元组处理
                .map(new MapFunction<String, Tuple1<Integer>>() {
                    @Override
                    public Tuple1<Integer> map(String value) throws Exception {
                        return Tuple1.of(Integer.parseInt(value));
                    }
                });
        //TODO: 3.设置会话滚动窗口，间隔时间为10秒，基于处理时间
        SingleOutputStreamOperator<Tuple1<Integer>> assignerDataStream = mapDataStream
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                //TODO: 4.设置窗口数据聚合
                .sum(0);

        //4.输出结果-sink
        assignerDataStream.print();

        //5.触发执行-execute
        env.execute(_06StreamSessionWindow.class.getName());
    }
}
