package com.test.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/4 19:49
 * @Desc: 使用 FLink 计算引擎实现离线批处理，读取文本文件并做 wordcount
 */
public class BatchWordcount {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        DataSource<String> inputDataSet = env.readTextFile("datas/wordcount.data");
        //3.处理数据-transformation
        //TODO: 3.1 把每行输出切割成单个单词
        FlatMapOperator<String, String> wordDataSet = inputDataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] arr = s.trim().split("\\s+");
                for (String word : arr) {
                    collector.collect(word);//将每个单词拆分出去
                }
            }
        });
        //TODO: 3.2 每个单词转换成二元组形式，使用 FLink 的特定元组类型实现
        MapOperator<String, Tuple2<String, Integer>> tupleDataSet = wordDataSet.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        //TODO: 3.3 对每个元素做分组并统计
        AggregateOperator<Tuple2<String, Integer>> resultDateSet = tupleDataSet.groupBy(0).sum(1);
        // 4.输出结果-sink
        resultDateSet.print();
        //5.触发执行-execute,批处理不需要触发执行


    }
}
