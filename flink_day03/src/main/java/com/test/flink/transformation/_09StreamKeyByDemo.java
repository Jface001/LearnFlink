package com.test.flink.transformation;

import com.test.flink.start._01StreamWordCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/7 20:36
 * @Desc:
 基于 Flink 流计算引擎：从TCP Socket消费数据，实时词频统计WordCount
flatMap（直接二元组）和KeySelector选择器、sum函数聚合
 */
public class _09StreamKeyByDemo {

    /**
     * 定义内部静态类，实现 FlatMapFunction 接口，将每一行数据切换成单个单词并转换成 二元组
     */
    private static class SplitterFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : value.trim().split("\\s+")) {
                Tuple2<String, Integer> tuple = Tuple2.of(word, 1);
                out.collect(tuple);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.准备数据-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1", 9999);
        //3.处理数据-transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = inputDataStream
                //TODO: 3.1 过滤数据，使用 Java 的 Lambda 表达式
                .filter(line -> null != line && line.trim().length() > 0)
                //TODO: 3.2 切割扁平化并转换为二元组，自定义静态类实现
                .flatMap(new SplitterFlatMap())
                //TODO: 3.3 按照单词分组， keyBy
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;// 按照单个单词分组，
                    }
                })
                //TODO: 3.4 聚合操作 sum
                .sum(1);
        //4.输出结果-sink
        resultDataStream.printToErr();
        //5.触发执行-execute,流式计算都需要触发
        env.execute(_09StreamKeyByDemo.class.getName());

    }
}
