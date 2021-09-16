package com.test.flink.transformation;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author: Jface
 * @Date: 2021/9/6 11:55
 * @Desc: Flink 批处理DataSet API中分区函数，rebalance、partitionBy
 */
public class BatchRepartitionDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //2.准备数据-source
        List<Tuple3<Integer, Long, String>> list = new ArrayList<>();
        list.add(Tuple3.of(1, 1L, "Hello"));
        list.add(Tuple3.of(2, 2L, "Hello"));
        list.add(Tuple3.of(3, 2L, "Hello"));
        list.add(Tuple3.of(4, 3L, "Hello"));
        list.add(Tuple3.of(5, 3L, "Hello"));
        list.add(Tuple3.of(6, 3L, "hehe"));
        list.add(Tuple3.of(7, 4L, "hehe"));
        list.add(Tuple3.of(8, 4L, "hehe"));
        list.add(Tuple3.of(9, 4L, "hehe"));
        list.add(Tuple3.of(10, 4L, "hehe"));
        list.add(Tuple3.of(11, 5L, "hehe"));
        list.add(Tuple3.of(12, 5L, "hehe"));
        list.add(Tuple3.of(13, 5L, "hehe"));
        list.add(Tuple3.of(14, 5L, "hehe"));
        list.add(Tuple3.of(15, 5L, "hehe"));
        list.add(Tuple3.of(16, 6L, "hehe"));
        list.add(Tuple3.of(17, 6L, "hehe"));
        list.add(Tuple3.of(18, 6L, "hehe"));
        list.add(Tuple3.of(19, 6L, "hehe"));
        list.add(Tuple3.of(20, 6L, "hehe"));
        list.add(Tuple3.of(21, 6L, "hehe"));
        // 将数据打乱，进行洗牌
        Collections.shuffle(list);

        DataSource<Tuple3<Integer, Long, String>> inputDataSet = env.fromCollection(list);

        //3.处理数据-transformation
        //TODO：指定字段,按照hash进行分区
        MapOperator<Tuple3<Integer, Long, String>, String> hashByDataSet = inputDataSet
                .partitionByHash(2)//按照哪个字段分区，都是 H 开头，在一个分区
                .map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
                    @Override
                    public String map(Tuple3<Integer, Long, String> value) throws Exception {
                        return getRuntimeContext().getIndexOfThisSubtask() + ":" + value.toString();
                    }
                });
        hashByDataSet.printToErr();
        //TODO:指定字段，按照Range范围进行分区
        MapOperator<Tuple3<Integer, Long, String>, String> rangeDataSet =
                inputDataSet.partitionByRange(0)
                        .map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
                            @Override
                            public String map(Tuple3<Integer, Long, String> value) throws Exception {
                                return getRuntimeContext().getIndexOfThisSubtask() + ":" + value.toString();
                            }
                        });
        rangeDataSet.printToErr();

        //TODO:自定义分区规则
        MapOperator<Tuple3<Integer, Long, String>, String> partitionerDataSet = inputDataSet
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, 0).map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
                    @Override
                    public String map(Tuple3<Integer, Long, String> value) throws Exception {
                        return getRuntimeContext().getIndexOfThisSubtask() + ":" + value.toString();
                    }
                });
        partitionerDataSet.printToErr();


    }
}
