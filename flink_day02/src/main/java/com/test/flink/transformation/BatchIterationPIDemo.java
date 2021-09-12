package com.test.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * @Author: Jface
 * @Date: 2021/9/6 10:16
 * @Desc: Flink中批处理DataSet 转换函数：Iteration 函数使用，计算圆周率
 */
public class BatchIterationPIDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        DataSource<Integer> inputDataSet = env.fromElements(0);
        //3.处理数据-transformation
        //TODO:迭代输入，设置迭代次数
        IterativeDataSet<Integer> iterateDataSet = inputDataSet.iterate(10000);
        //TODO:迭代函数，满足条件累加 1，否则为 0
        MapOperator<Integer, Integer> resultDataSet = iterateDataSet.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                //随机生成 Double 类型数值
                double x = Math.random();
                double y = Math.random();
                //判断x² + y²如果小于1，值为1，否则为0
                int sum = (x * x + y * y) < 1 ? 1 : 0;
                value += sum;
                return value;

            }
        });

        //TODO:获取迭代结果,计算圆周率
        DataSet<Integer> countDataSet = iterateDataSet.closeWith(resultDataSet);
        MapOperator<Integer, Double> reslutDataSet02 = countDataSet.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer value) throws Exception {
                double Pi = value / (double) 10000 * 4;
                return Pi;
            }
        });
        //4.数字终端-sink
        reslutDataSet02.printToErr();


    }
}
