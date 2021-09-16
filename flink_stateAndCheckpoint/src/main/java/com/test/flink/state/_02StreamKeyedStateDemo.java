package com.test.flink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Jface
 * @Date: 2021/9/16 12:20
 * @Desc: Flink State 中KeyedState，默认情况下框架自己维护，此外可以手动维护
 */
public class _02StreamKeyedStateDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.准备数据-source
        DataStreamSource<Tuple3<String, String, Long>> inputStream = env.fromElements(
                Tuple3.of("上海", "普陀区", 488L),
                Tuple3.of("上海", "徐汇区", 212L),
                Tuple3.of("北京", "西城区", 823L),
                Tuple3.of("北京", "海淀区", 234L),
                Tuple3.of("上海", "杨浦区", 888L),
                Tuple3.of("上海", "浦东新区", 666L),
                Tuple3.of("北京", "东城区", 323L),
                Tuple3.of("上海", "黄浦区", 111L)
        );
        //3.处理数据-transformation
        //TODO：框架自己维护 State，使用DataStream转换函数max获取每个市最大值
        SingleOutputStreamOperator<Tuple3<String, String, Long>> resultStream = inputStream.keyBy(0).sum(2);

        //TODO：自己管理KeyedState，存储每个 Key 状态 State
        //3.1 按照 Key 分组
        SingleOutputStreamOperator<String> resultStream02 = inputStream.keyBy(0)
                //3.2 获取 Key 中最大值
                .map(new RichMapFunction<Tuple3<String, String, Long>, String>() {
                    //3.2.1 定义变量，存储状态
                    private ValueState<Long> maxState = null;

                    //3.2.2 open 方法里面初始化状态
                    @Override
                    public void open(Configuration conf) throws Exception {
                        maxState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("maxState", Long.class));
                    }

                    //3.2.3 map 方法处理状态逻辑
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        //a. 获取以前的状态
                        Long preValue = maxState.value();
                        //b. 获取当前的状态
                        Long nowValue = value.f2;
                        //c.判断是否为首次计算，和状态值和传值大小
                        if (null == preValue || nowValue > preValue) {
                            maxState.update(nowValue);
                        }
                        //3.2.4 返回处理后的结果
                        return value.f0 + "，" + maxState.value();

                    }


                });


        //4.输出结果-sink
        resultStream02.printToErr();
        //5.触发执行-execute
        env.execute(_02StreamKeyedStateDemo.class.getSimpleName());

    }
}
