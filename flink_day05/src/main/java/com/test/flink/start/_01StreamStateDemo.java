package com.test.flink.start;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/15 22:58
 * @Desc: 使用Flink DataStream实现词频统计WordCount，从Socket Source读取数据。
 */
public class _01StreamStateDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 数据源-source
        DataStream<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);
        env.fromElements("spark flink flink") ;


        // 3. 数据转换-transformation
        // 3-1. 过滤数据，使用filter函数
        DataStream<String> lineStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                return null != line && line.trim().length() > 0;
            }
        });
        // 3-2. 转换数据，分割每行为单词
        DataStream<String> wordStream = lineStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.trim().toLowerCase().split("\\s+");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        // 3-3. 转换数据，每个单词变为二元组
        DataStream<Tuple2<String, Integer>> tupleStream = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        // 3-4. 分组
        DataStream<Tuple2<String, Integer>> resultStream = tupleStream
                .keyBy(0)
                .sum(1);

        // 4. 数据终端-sink
        resultStream.printToErr();

        // 5. 触发执行-execute
        env.execute("StreamWordCount");

    }
}
