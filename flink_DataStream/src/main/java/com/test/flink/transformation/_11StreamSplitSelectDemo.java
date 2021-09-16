package com.test.flink.transformation;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * @Author: Jface
 * @Date: 2021/9/7 21:01
 * @Desc: Flink 流计算中转换函数：split流的拆分和select流的选择
 */
public class _11StreamSplitSelectDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Long> dataStream = env.generateSequence(1, 10);

        // 3. 数据转换-transformation
        String oddTag = "odd";
        String evenTag = "even";
        //TODO: 1. 按照奇数和偶数分割流
        SplitStream<Long> splitDataStream = dataStream.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                if (value % 2 == 0) {
                    return Collections.singleton(evenTag);
                }
                return Collections.singleton(oddTag);
            }
        });
        //TODO: 2. 使用 select 函数，依据名称获取分割流
        DataStream<Long> evenDataStream = splitDataStream.select(evenTag);
        DataStream<Long> oddDataStream = splitDataStream.select(oddTag);

        //4. 数据终端-sink
        evenDataStream.printToErr("even>>>>");
        oddDataStream.print("odd>>>>");

        // 5. 触发执行-execute
        env.execute(_11StreamSplitSelectDemo.class.getSimpleName());


    }
}
