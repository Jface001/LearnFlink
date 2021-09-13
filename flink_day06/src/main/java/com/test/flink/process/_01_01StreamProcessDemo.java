package com.test.flink.process;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/13 15:28
 * @Desc: 使用Flink 计算引擎实现流式数据处理：从Socket接收数据，实时进行词频统计WordCount
 */
public class _01_01StreamProcessDemo {
    public static void main(String[] args)throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1", 9999);
        //3.处理数据-transformation
        //TODO：使用 DataStream 中的 Filter 函数，Filter属于高级 API 函数
        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                return null != line && line.trim().length() > 0;
            }
        });


        //TODO：使用底层 API 方法，process
        SingleOutputStreamOperator<String> processStream = inputStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String line, Context ctx, Collector<String> out) throws Exception {
                if (null != line && line.trim().length() > 0) {
                    out.collect(line);
                }

            }
        });

        //4.输出结果-sink
        //filterStream.printToErr();
        processStream.printToErr();

        //5.触发执行-execute
        env.execute(_01_01StreamProcessDemo.class.getName());
    }
}
