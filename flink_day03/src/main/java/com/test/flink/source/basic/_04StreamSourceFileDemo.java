package com.test.flink.source.basic;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Jface
 * @Date: 2021/9/7 18:05
 * @Desc: Flink 流计算数据源：基于文件的Source
 */
public class _04StreamSourceFileDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        DataStreamSource<String> resultDataStream = env.readTextFile("datas/wordcount.data");
        resultDataStream.print();
        DataStreamSource<String> resultDataStream01 = env.readTextFile("datas/wordcount.data.gz");
        resultDataStream01.printToErr();
        //3.处理数据-transformation,不需要处理
        //4.触发执行-execute
        env.execute(_04StreamSourceFileDemo.class.getName());


    }
}
