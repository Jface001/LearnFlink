package com.test.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/4 20:14
 * @Desc: 使用 FLink 计算引擎实现实时流式数据处理，监听端口并做 wordcount
 */
public class StreamWordcount {
    public static void main(String[] args) throws Exception {
         //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         //2.准备数据-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("192.168.88.161", 9999);
        //3.处理数据-transformation
        //TODO: 切割成单个单词 flatmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataSet = inputDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] arr = value.trim().split("\\s+");
                for (String s : arr) {
                    out.collect(s);//将每个单词拆分出去
                }
            }
            //TODO: 单词--> 元组形式，map
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value,1);
            }
            //TODO: 分组聚合 keyBy & sum
        }).keyBy(0).sum(1);
        //4.输出结果-sink
        resultDataSet.print();
        //5.触发执行-execute
        env.execute(StreamWordcount.class.getSimpleName());
    }
}
