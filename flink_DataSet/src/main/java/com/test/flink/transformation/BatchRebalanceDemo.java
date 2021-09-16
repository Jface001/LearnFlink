package com.test.flink.transformation;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: Jface
 * @Date: 2021/9/6 11:42
 * @Desc: Flink 批处理DataSet API中分区函数 rebalance 重平衡函数使用
 */
public class BatchRebalanceDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //2.准备数据-source
        DataSource<Long> inputDataSet = env.generateSequence(1, 30);
        //3.处理数据-transformation
        //TODO:查看分区处理的数据
        MapOperator<Long, String> resultDataSet01 = inputDataSet.map(new RichMapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                return index + ":" + value;
            }
        });
        resultDataSet01.printToErr();

        //TODO:将各个分区数据不均衡，过滤 -> value > 5 and value < 26
        FilterOperator<Long> filterDataSet = inputDataSet.filter(new RichFilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 5 && value < 26;
            }
        });
        //TODO:再次查看分区处理的数据
        MapOperator<Long, String> resultDataSet02 = filterDataSet.map(new RichMapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return getRuntimeContext().getIndexOfThisSubtask() + ":" + value;
            }
        });
        System.out.println("=====================");
        resultDataSet02.printToErr();

        //TODO: 使用 rebalance 函数，对 DataSet 数据集数据进行重平衡
        PartitionOperator<Long> rebalanceDataSet = filterDataSet.rebalance();
        //TODO: 查看平衡之后的各个分区处理数据的情况
        MapOperator<Long, String> resultDataSet03 = rebalanceDataSet.map(new RichMapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return getRuntimeContext().getIndexOfThisSubtask() + ":" + value;
            }
        });
        System.out.println("=====================");
        resultDataSet03.printToErr();


    }
}
