package com.test.dataset;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/10/17 16:31
 * @Desc: 使用 FLink 开发 DataSet
 */
public class _02WordCount {
    public static void main(String[] args) throws Exception {
        //1.Env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.Source
        DataSource<String> inputDataSet = env.readTextFile("datas/wc.input");
        //3.Transformation
        //3.1 过滤
        AggregateOperator<Tuple2<String, Integer>> resultDataSet = inputDataSet
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return null != value && value.trim().length() > 0;
                    }
                })      //切割
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String s : value.trim().split("\\s+")) {
                            out.collect(s);
                        }
                    }
                })
                //转换二元组
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                //分组求和
                .groupBy(0).sum(1);

        //排序,并只取前三
        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> sortDataSet = resultDataSet.sortPartition("f1", Order.DESCENDING)
                .setParallelism(1)
                .first(3);
        //4.Sink
        sortDataSet.print();
        //5.Execute，批处理不需要
    }
}
