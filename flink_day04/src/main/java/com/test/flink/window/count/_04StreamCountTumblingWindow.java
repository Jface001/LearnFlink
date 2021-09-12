package com.test.flink.window.count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author: Jface
 * @Date: 2021/9/8 17:42
 * @Desc: 窗口统计案例演示：滚动计数窗口（Tumbling Count Window)，数字累加求和统计
 */
public class _04StreamCountTumblingWindow {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1", 9999);

        //3.处理数据-transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> windowDataStream = inputDataStream
                //TODO: 1.过滤脏数据
                .filter(line -> null != line && line.trim().split(",").length == 2)
                //TODO: 2.切割并封装到二元组
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] arr = value.trim().split(",");
                        String name = arr[0];
                        int counter = Integer.parseInt(arr[1]);
                        return Tuple2.of(name, counter);
                    }
                })
                //TODO: 3.设置计数滚动窗口，size=5
                .countWindowAll(5)
                //TODO: 4.设置窗口数据聚合
                .sum(1);
        //4.输出结果-sink
        windowDataStream.printToErr();
        //5.触发执行-execute
        env.execute(_04StreamCountTumblingWindow.class.getName());

    }
}
