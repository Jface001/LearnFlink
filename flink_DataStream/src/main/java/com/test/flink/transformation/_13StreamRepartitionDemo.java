package com.test.flink.transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Jface
 * @Date: 2021/9/8 12:28
 * @Desc:
 */
public class _13StreamRepartitionDemo {
    public static void main(String[] args) throws Exception{
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 数据源-source
        DataStreamSource<Tuple2<Integer, String>> dataStream = env.fromElements(
                Tuple2.of(1, "a"), Tuple2.of(2, "b"), Tuple2.of(3, "c"), Tuple2.of(4, "d"));

        // 3. 数据转换-transformation
        // TODO: 1、global函数，将所有数据发往1个分区Partition

        // globalDataStream.print();

        // TODO: 2、broadcast函数， 广播数据


        //broadcastDataStream.printToErr();
        // TODO: 3、forward函数，上下游并发一样时 一对一发送


        //forwardDataStream.print().setParallelism(1) ;
        // TODO: 4、shuffle函数，随机均匀分配


        // TODO: 5、rebalance函数，轮流分配

        //rebalanceDataStream.print() ;

        // TODO: 6、rescale函数，本地轮流分配

        //rescaleDataStream.printToErr();


        // TODO: 7、partitionCustom函数，自定义分区规则
        // 4. 数据终端-sink
        env.execute(_13StreamRepartitionDemo.class.getSimpleName());

    }
}
