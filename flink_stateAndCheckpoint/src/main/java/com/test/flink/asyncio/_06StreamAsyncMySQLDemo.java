package com.test.flink.asyncio;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Jface
 * @Date: 2021/9/16 20:20
 * @Desc: 采用异步方式请求MySQL数据库，此处使用 Async IO实现
 */
public class _06StreamAsyncMySQLDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        // 2. 数据源-source
        DataStream<String> dataStream = env.addSource(new ClickLogSource());
        //dataStream.printToErr();

        // 3. 数据转换-transformation
        // 3-1. 解析数据，获取userId，封装至二元组
        SingleOutputStreamOperator<Tuple2<String, String>> logStream = dataStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                // u_1007,browser,2021-09-01 16:25:19.793 -> u_1007
                String userId = value.split(",")[0];
                // 构建二元组
                Tuple2<String, String> tuple = Tuple2.of(userId, value);
                // 返回数据
                return tuple;
            }
        });

		/*
			u_1007,browser,2021-09-01 16:25:19.793   ->  sunqi,u_1007,browser,2021-09-01 16:25:19.793
		 */

        // 3-2. TODO: 异步请求MySQL，采用线程池方式请求
        DataStream<String> asyncStream = AsyncDataStream.unorderedWait(
                logStream, // 数据流
                new AsyncMySQLRequest(), // 异步请求方法实现
                1000,
                TimeUnit.MILLISECONDS,
                10
        );

        // 4. 数据终端-sink
        asyncStream.printToErr();

        // 5. 触发执行-execute
        env.execute("StreamAsyncMySQLDemo") ;
    }

    /**
     * 自定义数据源，实时产生用户行为日志数据
     */
    private static class ClickLogSource extends RichSourceFunction<String> {
        private boolean isRunning = true ;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            String[] array = new String[]{"click", "browser", "browser", "click", "browser", "browser", "search"};
            Random random = new Random();
            FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS") ;
            // 模拟用户点击日志流数据
            while (isRunning){
                String userId = "u_" + (1000 + random.nextInt(10)) ;
                String behavior = array[random.nextInt(array.length)] ;
                Long timestamp = System.currentTimeMillis();

                String output = userId + "," + behavior + "," + format.format(timestamp) ;
                System.out.println("source>>" + output);
                // 输出
                ctx.collect(output);
                // 每隔至少1秒产生1条数据
                TimeUnit.SECONDS.sleep( 1 + random.nextInt(2));
            }
        }

        @Override
        public void cancel() {
            isRunning = false ;
        }
    }
}
