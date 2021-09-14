package com.test.flink.table.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @Author: Jface
 * @Date: 2021/9/14 13:54
 * @Desc: Flink SQL API 针对批处理实现词频统计WordCount
 */
public class _02BatchWordCountSQLDemo {
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
        //3.1 将 DataSet 注册为临时视图
        tableEnv.createTemporaryView("tbl_wordcounts", inputDataSet, "word,counts");
        //3.2 编写 SQL 分析数据
        Table resultTable = tableEnv.sqlQuery("select word,sum(counts) as counts from tbl_wordcounts group by word");
        //3.3 转换 Table 为 DataSet
        DataSet<WordCount> resultDataSet = tableEnv.toDataSet(resultTable, WordCount.class);
        //4.输出结果-sink
        resultDataSet.printToErr();
        //5.触发执行-execute,上面自动触发执行了，这里可以不执行
        //env.execute(_02BatchWordCountSQLDemo.class.getName());

    }
}
