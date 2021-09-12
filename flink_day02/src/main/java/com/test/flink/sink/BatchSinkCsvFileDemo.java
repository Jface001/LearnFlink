package com.test.flink.sink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;


/**
 * @Author: Jface
 * @Date: 2021/9/5 19:49
 * @Desc: DataSet API 批处理中数据终端：基于文件Sink
 * 方式5：保存为 CSV文件，必须为元组形式才能保存
 */
public class BatchSinkCsvFileDemo {
    public static void main(String[] args) throws Exception {
        //1.执行环境 env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.数据源-source
        DataSource<String> dataSet = env.readTextFile("datas/wordcount.data");
        //3.数据终端-sink,保存为 CSV 文件
        //TODO:将输出转换为二元数组形式
        MapOperator<String, Tuple2<Integer, String>> resultDataSet = dataSet.map(new RichMapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String value) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(index, value);
            }
        });
        //TODO:保存到 CSV 文件,需要设置行分隔符和列分隔符
        resultDataSet.writeAsCsv("datas/write-csv", "\n", "$", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        //4.触发执行，有写出需要触发执行
        env.execute(BatchSinkCsvFileDemo.class.getName());


    }
}
