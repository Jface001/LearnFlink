package com.test.flink.process;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: Jface
 * @Date: 2021/9/13 15:39
 * @Desc: 使用Flink 计算引擎实现流式数据处理：从Socket接收数据
 * 实时进行词频统计WordCount，使用process函数处理
 */
public class _01_02StreamProcessDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.准备数据-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1", 9999);
        //3.处理数据-transformation
        //TODO：使用 DataStream API 实现
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputStream
                //过滤
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {
                        return null != line && line.trim().length() > 0;
                    }
                })
                //切割扁平线
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        for (String word : line.split("\\s+")) {
                            out.collect(word);
                        }

                    }
                })
                //转成二元组
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                })
                //分组并聚合
                .keyBy(0).sum(1);


        //TODO：使用底层 API 方法 process 实现 将行数据转换成二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> processStream = inputStream.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String line, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (null != line && line.trim().length() > 0) {
                    for (String word : line.trim().split("\\s+")) {
                        Tuple2<String, Integer> tuple = Tuple2.of(word, 1);
                        out.collect(tuple);
                    }
                }
            }
        });
        //TODO：使用 process 方法对聚合之后的单词做词频统计
        SingleOutputStreamOperator<String> resultStream02 = processStream.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, String>() {
                    //a. 定义状态变量
                    private ValueState<Integer> counterState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //b. 对状态进行初始化，从运行时上下文中获取状态
                        counterState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("counter", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        //c. 获取分组的 key
                        String key = value.f0;
                        //d. 从状态中获取上一次的词频
                        Integer last_value = counterState.value();
                        //e. 获取当前的词频
                        Integer current_value = value.f1;
                        //f. 合并上一次词频和当前词频，并更新到状态
                        if (null == last_value) {
                            counterState.update(current_value);
                        } else {
                            Integer total_value = last_value + current_value;
                            counterState.update(total_value);
                        }

                        //g. 输出当前的 key 和对应的词频
                        String ouput = key + " = " + counterState.value();
                        out.collect(ouput);
                    }
                });

        //4.输出结果-sink
        //resultStream.printToErr();
        resultStream02.printToErr();

        //5.触发执行-execute
        env.execute(_01_02StreamProcessDemo.class.getName());

    }
}
