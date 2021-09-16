package com.test.flink.state;

import com.fasterxml.jackson.databind.type.ResolvedRecursiveType;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.concurrent.TimeUnit;


/**
 * @Author: Jface
 * @Date: 2021/9/16 17:53
 * @Desc: Flink State 中 OperatorState，自定义数据源Kafka消费数据，保存消费偏移量数据并进行Checkpoint
 */

/**
 * 自定义数据源，模拟从 Kafka 消费数据，手动管理消费偏移量，并对状态进行 Checkpoint
 */
public class _03StreamOperatorStateDemo {

    /**
     * 设置Flink Stream流式应用Checkpoint相关属性
     */
    private static void setEnvironment(StreamExecutionEnvironment env) {
        //1. 每隔1s执行一次Checkpoint
        env.enableCheckpointing(1000);
        //2. 状态数据保存本地文件系统
        env.setStateBackend(new FsStateBackend("file:///D:/workspace/LearnFlink/datas/"));
        //3. 当应用取消时，Checkpoint数据保存，不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        //4. 设置模式Mode为精确性一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //5. 设置重启策略，不设置重启策略时，将会无限重启
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(3)));
    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为3
        env.setParallelism(3);
        //TODO：设置检查点Checkpoint相关属性，保存状态和应用重启策略
        setEnvironment(env);

        // 2. 数据源-source
        //TODO: 添加我们自定义的数据源
        DataStreamSource<String> inputStream = env.addSource(new KafkaSource());

        // 3. 数据转换-transformation
        // 4. 数据终端-sink
        inputStream.printToErr();

        // 5. 触发执行-execute
        env.execute("StreamOperatorStateDemo");
    }

    /**
     * 自定义数据源，模拟从 Kafka 消费数据，手动管理 State，手动管理 Checkpoint
     */
    private static class KafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        // 定义变量表示是否运行
        boolean isRunning = true;
        // 定义 Long 类型，表示消费偏移量
        Long offset = 0L;
        // 1. 定义状态，保存偏移量
        private ListState<Long> offsetState = null;

        // 2. 启动流式应用时，对状态进行初始化
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 状态初始化
            offsetState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>("offsetState", Long.class)
            );
            // Checkpoint 恢复，获取状态
            if (context.isRestored()) {
                offset = offsetState.get().iterator().next();
            }

        }

        //3.定时将状态进行快照和保存：checkpoint 操作
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清除之前状态
            offsetState.clear();
            //将当前消费的最新偏移量赋值，将 Long 类型转换为 singletonList 类型
            offsetState.addAll(Collections.singletonList(offset));

        }

        //4.数据处理的逻辑
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                //模拟 Kafka 消费，每次消费 offset+1
                int partitionId = getRuntimeContext().getIndexOfThisSubtask();
                String output = "P-" + partitionId + " -> " + (offset++);
                ctx.collect(output);
                //更新状态
                offsetState.update(Collections.singletonList(offset));
                //每隔一秒消费一次数据
                TimeUnit.SECONDS.sleep(1);
                //TODO：制裁异常，方便测试故障恢复 offset
                if (offset % 5 == 0) {
                    throw new RuntimeException("程序出现异常！！请及时处理");
                }
            }

        }

        @Override
        public void cancel() {
            isRunning = false;

        }
    }
}
