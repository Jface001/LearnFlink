package com.test.flink.transformation;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author: Jface
 * @Date: 2021/9/7 20:55
 * @Desc: Flink 流计算中转换函数：合并union和连接connect
 */
public class _10StreamUnionConnectDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 数据源-source
        DataStreamSource<String> dataStream01 = env.fromElements("A", "B", "C", "D");
        DataStreamSource<String> dataStream02 = env.fromElements("aa", "bb", "cc", "dd");
        DataStreamSource<Integer> dataStream03 = env.fromElements(1, 2, 3, 4);

        // 3. 数据转换-transformation
        // TODO: 两个流进行union
        DataStream<String> unionDataStream = dataStream01.union(dataStream02);
        // TODO: 两个流进行连接
        ConnectedStreams<String, Integer> connect = dataStream01.connect(dataStream03);
        SingleOutputStreamOperator<String> connectDataStream = connect.map(new CoMapFunction<String, Integer, String>() {
            //第一个流的计算操作
            @Override
            public String map1(String value) throws Exception {
                return "map1: " + value;
            }

            //第二个流的计算操作
            @Override
            public String map2(Integer value) throws Exception {
                return "map2: " + value;
            }
        });

        // 4. 数据终端-sink
        unionDataStream.printToErr();
        connectDataStream.print();

        // 5. 触发执行-execute
        env.execute(_10StreamUnionConnectDemo.class.getSimpleName());

    }
}
