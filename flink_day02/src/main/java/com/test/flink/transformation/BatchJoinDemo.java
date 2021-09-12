package com.test.flink.transformation;

import lombok.Data;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/6 10:36
 * @Desc: Flink 框架中批处理实现两个数据集关联分析：JOIN
 */
public class BatchJoinDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        DataSource<Score> scoreDataSet = env.readCsvFile("datas/score.csv")
                .fieldDelimiter(",")
                .pojoType(Score.class, "id", "stuName", "subId", "score");

        DataSource<Subject> subjectDataSet = env.readCsvFile("datas/subject.csv")
                .fieldDelimiter(",")
                .pojoType(Subject.class, "id", "name");
        //3.处理数据-transformation

        JoinOperator.EquiJoin<Score, Subject, String> joinDataSet = scoreDataSet
                //TODO: 指定关联数据集
                .join(subjectDataSet)
                //TODO: 指定关联条件
                .where("subId").equalTo("id")
                //TODO: 选择字段
                .with(new JoinFunction<Score, Subject, String>() {
                    @Override
                    public String join(Score score, Subject subject) throws Exception {
                        //获取右边中字段
                        String name = subject.getName();
                        //拼接字符串
                        String output = score.getId() + "," + score.getStuName() + "," + name + "," + score.getScore();
                        //返回结果
                        return output;
                    }
                });

        //4.输出结果-sink
        joinDataSet.printToErr();
        //5.触发执行-execute，没有写出，不需要触发

    }

    @Data
    public static class Score {
        private Integer id;
        private String stuName;
        private Integer subId;
        private Double score;
    }

    @Data
    public static class Subject {
        private Integer id;
        private String name;
    }


}
