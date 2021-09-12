package com.test.flink.source.kafka;

import lombok.Data;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;

/**
 * @Author: Jface
 * @Date: 2021/9/5 18:30
 * @Desc: DataSet API 批处理中数据源：基于文件Source
 * 1.env.readTextFile(本地文件/HDFS文件); //压缩文件也可以
 * 2.env.readCsvFile[泛型]("本地文件/HDFS文件")
 * 3.env.readTextFile("目录").withParameters(parameters);
 * Configuration parameters = new Configuration();
 * parameters.setBoolean("recursive.file.enumeration", true);//设置是否递归读取目录
 */
public class BatchSourceFileDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        //方式1：读取文本文件，可以是压缩
        DataSource<String> dataSet01 = env.readTextFile("datas/wordcount.data.gz");
        dataSet01.print();
        //方式2：读取 CSV 文件，设置分隔符，执行 POJO 对象
        DataSource<Rating> dataSet02 = env.readCsvFile("datas/u.data")
                //设置读取的时候分隔符
                .fieldDelimiter("\t")
                //设置解析的POJO类
                .pojoType(Rating.class, "userId", "movieId", "rating", "timestamp");
        dataSet02.print();
        //方式3：递归读取子目录中文件数据
        //创建参数
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);
        DataSource<String> dataSet03 = env.readTextFile("datas/subDatas")
                //设置参数
                .withParameters(parameters);
        dataSet03.print();
    }

    @Data
    public static class Rating {
        public Integer userId;
        public Integer movieId;
        public Double rating;
        public Long timestamp;
    }
}
