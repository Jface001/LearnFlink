package com.test.flink.start;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/8 17:12
 * @Desc: 基于 Flink 流计算引擎：从TCP Socket消费数据，实时词频统计WordCount
 */
public class _01WindowWordCount {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1", 9999);

        //3.处理数据-transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = inputDataStream
                //TODO: 1.过滤脏数据
                .filter(line -> null != line && line.trim().length() > 0)
                //TODO: 2.切割并扁平化
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String word : value.trim().split("\\s+")) {
                            out.collect(word);
                        }
                    }
                })
                //TODO: 3.转换成二元组
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                //TODO: 4.分组并求和
                .keyBy(0).sum(1);


        //4.输出结果-sink
        resultDataStream.print();

        //5.触发执行-execute
        env.execute(_01WindowWordCount.class.getName());


    }
}
