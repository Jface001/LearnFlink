package com.test.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * @Author: Jface
 * @Date: 2021/9/6 10:08
 * @Desc: Flink中批处理DataSet 转换函数：Iteration 函数使用
 */
public class BatchIterationDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        DataSource<Long> inputDataSet = env.generateSequence(1, 5);
        //3.处理数据-transformation

        // 迭代输入，设置迭代次数
        IterativeDataSet<Long> iterateDataSet = inputDataSet.iterate(10);
        // 迭代函数，每次迭代数字 + 1
        MapOperator<Long, Long> mapDataSet = iterateDataSet.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value + 1L;
            }
        });
        //获取迭代计算之后的结果
        DataSet<Long> resultDataSet = iterateDataSet.closeWith(mapDataSet);

        //4.数字终端- sink
        resultDataSet.printToErr();


    }
}
