package com.test.flink.table.strean;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: Jface
 * @Date: 2021/9/14 14:57
 * @Desc: Flink Table API使用，基于事件时间窗口统计分析
 */
public class _05StreamWindowTableDemo {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1.2 创建流计算Table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1", 9999);

        // 3. 数据转换-transformation
        // 3.1 过滤脏数据，转化数据封装到 Row 对象
        SingleOutputStreamOperator<Row> rowStream = inputStream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return null != value && value.trim().split(",").length == 4;
                    }
                })
                .map(new MapFunction<String, Row>() {
                    private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public Row map(String line) throws Exception {
                        String[] arr = line.trim().split(",");
                        String userId = arr[0];
                        String product = arr[1];
                        Integer amount = Integer.valueOf(arr[2]);
                        long orderTime = format.parse(arr[3]).getTime();
                        return Row.of(orderTime, userId, product, amount);
                    }
                })
                .returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.INT));
        // 3.2 指定事件时间字段，类型必须为 Long 类型,暂时不考虑乱序和延迟时间
        SingleOutputStreamOperator<Row> timeStream = rowStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
             @Override
             public long extractTimestamp(Row element) {
                 Object orderTime = element.getField(0);
                 return (long) orderTime;
             }
         });
        // 3.3 将 DataStream 转化为 Table
        // event_time.rowtime 固定写法，uev事件时间
        Table inputTable = tableEnv.fromDataStream(timeStream,
                "order_time, user_id, product, amount, event_time.rowtime");
        // 3.4 编写 Table API 进行窗口分析
        Table resultTable = inputTable
                // 设置窗口
                .window(
                        Tumble.over("5.seconds").on("event_time").as("win"))
                // 对窗口中的数据按照用户 id 分组
                .groupBy("win, user_id")
                .select(
                        "win.start as win_start, win.end as win_end, user_id, amount.sum as total "
                );

        // 3.5 table 转化为 DataStream
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);

        // 4. 数据终端-sink
        resultStream.printToErr();


        // 5. 触发执行-execute
        env.execute(_05StreamWindowTableDemo.class.getName());

    }
}
