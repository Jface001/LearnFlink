package com.test.flink.other;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: Jface
 * @Date: 2021/9/6 12:36
 * @Desc: Flink 批处理中广播变量：将小数据集广播至TaskManager内存中，便于使用
 */
public class BatchBroadcastDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        // 大表数据
        DataSource<Tuple3<Integer, String, Integer>> scoreDataSet = env.fromCollection(
                Arrays.asList(
                        Tuple3.of(1, "语文", 50),
                        Tuple3.of(1, "数学", 70),
                        Tuple3.of(1, "英语", 86),
                        Tuple3.of(2, "语文", 80),
                        Tuple3.of(2, "数学", 86),
                        Tuple3.of(2, "英语", 96),
                        Tuple3.of(3, "语文", 90),
                        Tuple3.of(3, "数学", 68),
                        Tuple3.of(3, "英语", 92)
                )
        );
        // 小表数据
        DataSource<Tuple2<Integer, String>> studentDataSet = env.fromCollection(
                Arrays.asList(
                        Tuple2.of(1, "张三"),
                        Tuple2.of(2, "李四"),
                        Tuple2.of(3, "王五")
                )
        );
        //3.转换操作-transformation
        MapOperator<Tuple3<Integer, String, Integer>, String> resultDataSet = scoreDataSet.map(new RichMapFunction<Tuple3<Integer, String, Integer>, String>() {
            //TODO: 定义 Map 集合，存储广播变量，方便依据 key 获取 value 值
            private HashMap<Integer, String> stuMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO: 获取广播的数据集
                List<Tuple2<Integer, String>> list = getRuntimeContext().getBroadcastVariable("students");
                //TODO: 使用广播数据集，将数据放入的 Map 集合中
                for (Tuple2<Integer, String> tuples : list) {
                    stuMap.put(tuples.f0, tuples.f1);
                }
            }

            @Override
            public String map(Tuple3<Integer, String, Integer> value) throws Exception {
                Integer studentId = value.f0;
                String subjectName = value.f1;
                Integer score = value.f2;
                //根据学生id 获取名称
                String studentName = stuMap.get(studentId);
                return studentName + "," + subjectName + "," + score;
            }
        });
        //4. 数据终端-sink
        //TODO: 将小表数据广播出去
        MapOperator<Tuple3<Integer, String, Integer>, String> resultDataSet02 = resultDataSet.withBroadcastSet(studentDataSet, "students");
        resultDataSet02.printToErr();

    }
}
