package com.test.flink.start;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/5 12:37
 * @Desc: 基于Flink引擎实现批处理词频统计WordCount：过滤filter、排序sort等操作
 */
public class _01WordCount {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        DataSource<String> inputDataSet = env.readTextFile("datas/wc.input");
        //3.处理数据-transformation
        //TODO: 3.1 过滤脏数据
        AggregateOperator<Tuple2<String, Integer>> resultDataSet = inputDataSet.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                return null != line && line.trim().length() > 0;
            }
        })
                //TODO: 3.2 切割
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        for (String s : line.trim().split("\\s+")) {
                            out.collect(s);
                        }
                    }
                })
                //TODO: 3.3 转换二元组
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                })
                //TODO: 3.4 分组求和
                .groupBy(0).sum(1);
        //4.输出结果-sink
        resultDataSet.printToErr();
        //TODO: sort 排序，全局排序需要设置分区数
        SortPartitionOperator<Tuple2<String, Integer>> sortDataSet = resultDataSet.sortPartition("f1", Order.DESCENDING)
                .setParallelism(1);
        sortDataSet.printToErr();
        //只选择前3的数据
        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> resultDataSet2 = sortDataSet.first(3);
        resultDataSet2.print();

        //5.触发执行-execute，没有写出不需要触发执行

    }
}
