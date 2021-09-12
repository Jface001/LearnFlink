package com.test.flink.sink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.List;

/**
 * @Author: Jface
 * @Date: 2021/9/5 19:20
 * @Desc: DataSet API 批处理中数据终端：基于文件Sink
 * 1.ds.print 直接输出到控制台
 * 2.ds.printToErr() 直接输出到控制台,用红色
 * 3.ds.collect 将分布式数据收集为本地集合
 * 4.ds.setParallelism(1).writeAsText("本地/HDFS的path",WriteMode.OVERWRITE)
 * <p>
 * 注意: 在输出到path的时候，可以在前面设置并行度，如果
 * 并行度>1，则path为目录
 * 并行度=1，则path为文件名
 */
public class BatchSinkFileDemo {
    public static void main(String[] args) throws Exception {
        //1.执行环境 env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.数据源-source
        DataSource<String> dataSet = env.readTextFile("datas/wordcount.data");
        //3.数据终端-sink
        //方式1、方式2：控制台
        dataSet.print();
        dataSet.printToErr();
        System.out.println("======");
        //方式3：转换成本地集合
        List<String> list = dataSet.collect();
        System.out.println(list);
        System.out.println("======");
        //方式4：保存为文本文件，需要设置读写方式，重写或者追加等，分区数几个就有几个文件
        dataSet.writeAsText("datas/sink-text.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        System.out.println("======");
        //4.触发执行,有写出需要触发
        env.execute(BatchSinkFileDemo.class.getName());
    }
}
