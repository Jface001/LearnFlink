package com.test.flink.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: Jface
 * @Date: 2021/9/6 11:04
 * @Desc: Flink 框架中批处理实现两个数据集关联分析：LeftJoin
 */
public class BatchLeftJoinDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        DataSource<Tuple2<Integer, String>> nameDataSet = env.fromElements(Tuple2.of(1, "李白"), Tuple2.of(2, "杜甫"), Tuple2.of(3, "苏轼"));
        DataSource<Tuple2<Integer, String>> cityDataSet = env.fromElements(Tuple2.of(1, "北京"), Tuple2.of(2, "上海"), Tuple2.of(4, "广州"));
        //3.处理数据-transformation
        //TODO:指定关联数据集
        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> resultDataSet = nameDataSet.leftOuterJoin(cityDataSet)
                //TODO:指定条件
                .where(0).equalTo(0)
                //TODO:选择字段
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    @Override
                    public String join(Tuple2<Integer, String> name, Tuple2<Integer, String> city) throws Exception {
                        String cityName = "未知";
                        if (null != city) {
                            cityName = city.f1;
                        }
                        //TODO:返回结果
                        return name.f0 + "," + cityName + "," + name.f1;
                    }
                });

        //4.输出终端
        resultDataSet.printToErr();

    }
}
