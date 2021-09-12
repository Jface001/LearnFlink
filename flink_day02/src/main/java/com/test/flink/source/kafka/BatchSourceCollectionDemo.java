package com.test.flink.source.kafka;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

/**
 * @Author: Jface
 * @Date: 2021/9/5 18:23
 * @Desc: DataSet API 批处理中数据源：基于集合Source
 * 1.env.fromElements(可变参数);
 * 2.env.fromColletion(各种集合);
 * 3.env.generateSequence(开始,结束);
 */
public class BatchSourceCollectionDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.准备数据-source
        //2.1 可变参数
        DataSource<String> dataSet01 = env.fromElements("spark", "flink", "Hive");
        dataSet01.print();
        //2.2  Java集合对象
        DataSource<String> dataSet02 = env.fromCollection(Arrays.asList("spark", "flink", "Hive"));
        dataSet02.print();
        //2.3  自动生成序列（整形）
        DataSource<Long> dataSet03 = env.generateSequence(1, 10);
        dataSet03.print();
    }
}
