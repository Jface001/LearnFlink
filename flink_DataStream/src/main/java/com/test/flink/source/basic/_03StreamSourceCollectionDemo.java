package com.test.flink.source.basic;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author: Jface
 * @Date: 2021/9/7 17:59
 * @Desc: Flink 流计算数据源：基于集合的Source，分别为可变参数、集合和自动生成数据
 */
public class _03StreamSourceCollectionDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.准备数据-source
        //TODO: 可变参数
        DataStreamSource<String> resultDataStream01 = env.fromElements("spark", "flink", "hive");
        resultDataStream01.print();
        //TODO: 集合对象
        DataStreamSource<String> resultDataStream02 = env.fromCollection(Arrays.asList("spark", "flink", "hive"));
        resultDataStream02.printToErr();
        //TODO: 自动生成序列数字
        DataStreamSource<Long> resultDataStream03 = env.generateSequence(1, 10);
        resultDataStream03.print();
        //3.处理数据-transformation,不处理
        //4.触发执行-execute
        env.execute(_03StreamSourceCollectionDemo.class.getName());

    }
}
