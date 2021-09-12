package com.test.flink.other;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * @Author: Jface
 * @Date: 2021/9/6 12:19
 * @Desc:
 * 练习Flink中累加器Accumulator使用，统计处理的条目数
 * 累加器应用到转换函数中定义和使用的，当Job执行完成以后，可以获取值
 * 必须使用Rich富函数
 */
public class BatchAccumulatorDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据-source
        DataSource<String> inputDataSet = env.readTextFile("datas/click.log");
        //3.转换操作-transformation
        MapOperator<String, String> counterDataSet = inputDataSet.map(new RichMapFunction<String, String>() {
            //TODO:定义累加器
            private IntCounter counter = new IntCounter();

            //TODO:注册累加器
            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("counter", counter);
            }

            @Override
            public String map(String value) throws Exception {

                //TODO:使用累加器
                counter.add(1);
                return value;
            }

        });
        //4.数据终端-sink
        DataSink<String> resultDataSet = counterDataSet.writeAsText("datas/click.txt", FileSystem.WriteMode.OVERWRITE);
        //5.触发-execute
        JobExecutionResult jobExecutionResult = env.execute(BatchAccumulatorDemo.class.getName());
        //TODO:从任务执行结果中获取累加器结果
        Object counter = jobExecutionResult.getAccumulatorResult("counter");
        System.out.println("counter:" + counter);
    }
}
