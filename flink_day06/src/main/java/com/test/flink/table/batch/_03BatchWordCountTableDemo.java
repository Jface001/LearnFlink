package com.test.flink.table.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @Author: Jface
 * @Date: 2021/9/14 14:11
 * @Desc: Flink SQL API 针对批处理实现词频统计WordCount
 */
public class _03BatchWordCountTableDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.5 获取 Table 的执行环境
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        //2.准备数据-source
        DataSource<WordCount> inputDataSet = env.fromElements(
                new WordCount("flink", 1L), new WordCount("flink", 1L),
                new WordCount("spark", 1L), new WordCount("spark", 1L),
                new WordCount("flink", 1L), new WordCount("hive", 1L),
                new WordCount("flink", 1L), new WordCount("spark", 1L)
        );
        //3.处理数据-transformation
        //3.1 将 DataSet 转化成 Table
        Table inputTable = tableEnv.fromDataSet(inputDataSet);
        //3.2 编写 DSL分析数据
        Table resultTable = inputTable
                .groupBy("word")
                .select("word,sum(counts) as counts")
                .orderBy("counts.desc");
        //3.3 将 Table 转化为 DataSet
        DataSet<WordCount> resultDataSet = tableEnv.toDataSet(resultTable, WordCount.class);

        //4.数据终端-sink
        resultDataSet.printToErr();

        //5.触发执行-execute,上面自动触发执行了，这里不需要执行

    }
}
