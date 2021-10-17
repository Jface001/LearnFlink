package com.test.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/10/17 17:48
 * @Desc: 于 Flink 流计算引擎：从TCP Socket消费数据，实时词频统计WordCount
 */
public class _02WordCount {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.准备数据-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1", 9999);
        //3.处理数据-transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = inputDataStream
                //TODO: 过滤
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return null != value && value.trim().length() > 0;
                    }
                })
                //TODO: 切割和扁平化
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String word : value.trim().split("\\s+")) {
                            out.collect(word);
                        }
                    }
                })
                //TODO: 转换成二元组
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                //TODO: 分组
                .keyBy(0)
                //TODO:求和
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> temp, Tuple2<String, Integer> item) throws Exception {
                        String key = temp.f0;
                        int total = temp.f1 + item.f1;
                        return Tuple2.of(key, total);
                    }
                });

        //4.输出结果-sink
        resultDataStream.printToErr();
        //5.触发执行-execute,流式计算都需要触发
        env.execute(_02WordCount.class.getName());
    }
}
