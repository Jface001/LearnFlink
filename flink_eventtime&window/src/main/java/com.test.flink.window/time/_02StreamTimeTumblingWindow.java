package com.test.flink.window.time;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * @Author: Jface
 * @Date: 2021/9/8 17:25
 * @Desc: 窗口统计案例演示：时间滚动窗口（Time Tumbling  Window），实时交通卡口车流量统计
 */
public class _02StreamTimeTumblingWindow {
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
                //TODO: 3.按照卡口分组
                .keyBy(0)
                //TODO: 4.设置窗口,窗口时间为 5 秒
                .timeWindow(Time.seconds(5))
                //TODO: 5.设置窗口数据聚合
                .sum(1);
        //4.输出结果-sink
        windowDataStream.printToErr();

        //5.触发执行-execute
        env.execute(_02StreamTimeTumblingWindow.class.getName());

    }
}
