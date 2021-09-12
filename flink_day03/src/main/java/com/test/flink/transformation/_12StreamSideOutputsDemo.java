package com.test.flink.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.OutputStream;

/**
 * @Author: Jface
 * @Date: 2021/9/8 8:26
 * @Desc: Flink 流计算中转换函数：使用侧边流SideOutputs
 */
public class _12StreamSideOutputsDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Long> dataStream = env.generateSequence(1, 10);
        // 3. 数据转换-transformation
        // TODO： step1、定义侧边流标签Tag，需要泛型，需要大括号
        OutputTag<String> oddTag = new OutputTag<String>("odd_output") {
        };
        OutputTag<String> evenTag = new OutputTag<String>("even_output") {
        };

        // TODO：step2、调用DataStream#process方法，数据进行处理和判断打标签
        SingleOutputStreamOperator<String> processDataStream = dataStream.process(new ProcessFunction<Long, String>() {
            @Override
            public void processElement(Long value, Context ctx, Collector<String> out) throws Exception {
                //TODO：  step3、主流正常输出
                out.collect("main: " + value);
                // TODO：step4、处理侧边流
                if (value % 2 == 0) {
                    ctx.output(evenTag, "even: " + value);
                } else {
                    ctx.output(oddTag, "odd: " + value);
                }
            }
        });

        //4、输出终端-sink
        // TODO：step5、获取侧边栏主流输出
        processDataStream.print();
        processDataStream.getSideOutput(oddTag).printToErr();
        processDataStream.getSideOutput(evenTag).print();

        // 5. 触发执行-execute
        env.execute(_12StreamSideOutputsDemo.class.getSimpleName());

    }
}
