package com.test.flink.checkpoint;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Jface
 * @Date: 2021/9/16 20:01
 * @Desc: Flink Checkpoint定时保存状态State，演示案例
 */
public class _04StreamCheckpointDemo {
    /**
     * 定义方法，设置 Checkpoint 属性
     * @param env  执行环境
     * @param args 传参入口
     */
    private static void setEnvironment(StreamExecutionEnvironment env, String[] args){
        /* TODO： ================================== 建议必须设置 ================================== */
    // a. 设置Checkpoint-State的状态后端为FsStateBackend，本地测试时使用本地路径，集群测试时使用传入的HDFS的路径
        //env.setStateBackend(new FsStateBackend("hdfs://node1.:8020/flink-checkpoints/checkpoint"));
        if(args.length<1){
            env.setStateBackend(new FsStateBackend("file:///D:/workspace/LearnFlink/datas/ckp/"));
        }else {
            // 集群上执行可以传参数：hdfs://node1:8020/flink-checkpoints/checkpoint
            env.setStateBackend(new FsStateBackend(args[0]));

        }

    /*
    b. 设置Checkpoint时间间隔为1000ms，意思是做 2 个 Checkpoint 的间隔为1000ms。
    Checkpoint 做的越频繁，恢复数据时就越简单，同时 Checkpoint 相应的也会有一些IO消耗。
    */
        env.enableCheckpointing(1000);// 默认情况下如果不设置时间checkpoint是没有开启的
    /*
    c. 设置两个Checkpoint 之间最少等待时间，如设置Checkpoint之间最少是要等 500ms
    为了避免每隔1000ms做一次Checkpoint的时候，前一次太慢和后一次重叠到一起去了
    如:高速公路上，每隔1s关口放行一辆车，但是规定了两车之前的最小车距为500m
    */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    // d. 设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是 false不是
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
    // 设置Checkpoint时失败次数，允许失败几次
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
    /*
    e. 设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false，当作业被取消时，保留外部的checkpoint
    ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
    */
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        /* TODO： ================================== 直接使用默认的即可 ================================== */
    // a. 设置checkpoint的执行模式为EXACTLY_ONCE(默认)，注意:需要外部支持，如Source和Sink的支持
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    // b. 设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);
    // c. 设置同一时间有多少个checkpoint可以同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);// 默认为1
    }

    /**
     * 定义方法，调用函数 实现词频统计 WordCount
     * @param dataStream
     * @return
     */
    private static DataStream<Tuple2<String,Integer>> processStream (DataStream<String> dataStream) {
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return null != value && value.trim().length() > 0;
            }
        })
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.trim().split("\\s+")) {
                            Tuple2<String, Integer> tuple = Tuple2.of(word, 1);
                            out.collect(tuple);
                        }
                    }
                })
                .keyBy(0).sum(1);

        //返回结果
        return resultStream;
    }

    public static void main(String[] args) throws Exception{
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;
        // TODO： 检查点Checkpoint设置
        setEnvironment(env,args);

        // 2. 数据源-source
        DataStream<String> inputDataStream = env.addSource(new MySource());

        // 3. 数据转换-transformation
        DataStream<Tuple2<String, Integer>> resultDataStream = processStream(inputDataStream) ;

        // 4. 数据终端-sink
        resultDataStream.printToErr();

        // 5. 触发执行-execute
        env.execute(_04StreamCheckpointDemo.class.getSimpleName()) ;
    }

    /**
     * 自定义数据源，每隔一定时间产生字符串：
     */
    private static class MySource extends RichParallelSourceFunction<String> {
        private boolean isRunning = true ;
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning){
                // 发送数据
                ctx.collect("spark flink");

                // 每隔1秒产生一条数据
                TimeUnit.SECONDS.sleep(1);
            }
        }
        @Override
        public void cancel() {
            isRunning = false ;
        }
    }

}
