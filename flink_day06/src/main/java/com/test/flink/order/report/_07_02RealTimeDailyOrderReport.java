package com.test.flink.order.report;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Properties;

/**
 * @Author: Jface
 * @Date: 2021/9/11 19:21
 * @Desc: 实时订单报表：从Kafka Topic实时消费订单数据，进行销售订单额统计，结果实时存储MySQL数据库，维度如下：
 * - 第一、总销售额：sum
 * - 第二、各省份销售额：province
 * - 第三、重点城市销售额：city
 * "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市"
 */
public class _07_02RealTimeDailyOrderReport {
    /**
     * 1.自定义 JavaBean 对象，用于解析 JSON 字符串
     */
    @Setter
    @Getter
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderData {
        String orderId;
        String userId;
        String orderTime;
        String ip;
        Double orderMoney;
        Integer orderStatus;
        String province;
        String city;
        BigDecimal orderAmt;

        @Override
        public String toString() {
            return orderId + "," + userId + "," + orderTime + "," + ip + "," + orderMoney + "," + orderStatus;
        }
    }

    /**
     * 1.5 自定义 JavaBean 对象，用于存储消费 Kafka 之后返回的结果
     */
    @Setter
    @Getter
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderReport {
        String windowStart;
        String windowEnd;
        String typeName;
        Double totalAmt;

        @Override
        public String toString() {
            return windowStart + "~" + windowEnd + ": " + typeName + " = " + totalAmt;
        }
    }


    /**
     * 2.定义一个方法，用于设置Checkpoint检查点属性，Flink Stream流式应用，
     * 一共 9 个属性设置
     *
     * @param env
     */
    private static void setEnvCheckPoint(StreamExecutionEnvironment env) {
        // 1. 设置Checkpoint时间间隔
        env.enableCheckpointing(5000);

        // 2. 设置状态后端，记得使用 “ file:/// ”
        env.setStateBackend(new FsStateBackend("file:///D:\\workspace\\LearnFlink\\datas/flink-checkpoints/"));

        // 3. 设置两个Checkpoint 之间最少等待时间，
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // 4-1. 设置如果在做Checkpoint过程中出现错误，是否让整体任务失败
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);

        // 4-2. 设置Checkpoint时失败次数，允许失败几次
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // 5. 设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 6. 设置checkpoint的执行模式为EXACTLY_ONCE(默认)，注意：需要外部支持，如Source和Sink的支持
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 7. 设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 8. 设置同一时间有多少个checkpoint可以同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 9. 设置重启策略：NoRestart，生产环境需要设置次数和间隔时间
        env.setRestartStrategy(RestartStrategies.noRestart());

    }

    /**
     * 3.定义一个方法，用于从 kafka 实时消费数据，返回 DataStream，数据类型为 String
     *
     * @param env   执行环境
     * @param topic 消费主题
     * @return 返回一个 kafka 数据流
     */
    private static DataStream<String> kafkaSource(StreamExecutionEnvironment env, String topic) {
        //a. 消费 Kafka 数据时，指定属性参数
        Properties pros = new Properties();
        // kafka 集群通信地址
        pros.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        //消费者组 id
        pros.setProperty("group.id", "gid_10001");
        //设置分区发现
        pros.setProperty("flink.partition-discovery.interval-millis", "5000");
        //b. 构建 FlinkConsumer 实例
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), pros);
        //设置消费offset，从最新的位置开始消费
        kafkaConsumer.setStartFromLatest();

        //c. 添加数据源，获取返回的数据源
        DataStreamSource<String> kafkaStreamSource = env.addSource(kafkaConsumer);
        return kafkaStreamSource;
    }

    ;

    /**
     * 4.解析从Kafka消费获取的交易订单数据，过滤订单状态为0（打开）数据，并解析IP地址为省份和城市
     *
     * @param stream 输入的 kafka 数据源
     * @return 返回一个数据流
     */
    private static DataStream<OrderData> streamETL(DataStream<String> stream) {
        SingleOutputStreamOperator<OrderData> orderStream = stream
                //a. 解析 JSON 数据，封装实体类对象
                .map(new RichMapFunction<String, OrderData>() {
                    @Override
                    public OrderData map(String value) throws Exception {
                        return JSON.parseObject(value, OrderData.class);
                    }
                })
                //b. 过滤订单状态为 0 的数据
                .filter(new RichFilterFunction<OrderData>() {
                    @Override
                    public boolean filter(OrderData order) throws Exception {
                        return 0 == order.getOrderStatus();
                    }
                })
                //c. 解析 IP 为省份和城市
                .map(new RichMapFunction<OrderData, OrderData>() {
                    //定义变量
                    private DbSearcher dbSearcher = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dbSearcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db");
                    }

                    @Override
                    public OrderData map(OrderData order) throws Exception {
                        //获取 IP
                        String ip = order.getIp();
                        //解析 IP，获取省份和城市
                        DataBlock dataBlock = dbSearcher.btreeSearch(ip);
                        String[] arr = dataBlock.getRegion().split("\\|");
                        order.setProvince(arr[2]);
                        order.setCity(arr[3]);
                        //获取订单金额,转换为 BigDecimal 类型
                        Double orderMoney = order.getOrderMoney();
                        BigDecimal total = new BigDecimal(orderMoney).setScale(2, RoundingMode.HALF_UP);
                        order.setOrderAmt(total);
                        //d. 返回实体类对象 order
                        return order;
                    }
                });
        //返回数据流
        return orderStream;
    }

    /**
     * 5.实时报表统计：每日总销售额
     *
     * @param stream 清洗过后的数据流
     * @return 返回一个二元组，包含统计类型和总金额
     */
    private static DataStream<OrderReport> reportDailyGlobal(DataStream<OrderData> stream) {
        //a. 设置事件时间字段，类型为 Long，允许最大乱序时间为 2 秒
        SingleOutputStreamOperator<OrderReport> resultStream = stream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<OrderData>(Time.seconds(2)) {
                    private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

                    @Override
                    public long extractTimestamp(OrderData order) {
                        String orderTime = order.getOrderTime();
                        Long timeStamp = System.currentTimeMillis();
                        try {
                            timeStamp = format.parse(orderTime).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return timeStamp;
                    }
                }
        )
                //b. 提前字段：订单金额和添加字段：全国,二元组
                .map(new RichMapFunction<OrderData, Tuple2<String, BigDecimal>>() {
                    @Override
                    public Tuple2<String, BigDecimal> map(OrderData order) throws Exception {
                        BigDecimal orderAmt = order.getOrderAmt();
                        return Tuple2.of("全国", orderAmt);
                    }
                })
                //c. 分组，设置窗口、触发器，做聚合
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .apply(new RichWindowFunction<Tuple2<String, BigDecimal>, OrderReport, Tuple, TimeWindow>() {
                    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, BigDecimal>> input, Collector<OrderReport> out) throws Exception {
                        //a. 获取分组字段
                        Tuple1<String> tupleType = (Tuple1<String>) tuple;
                        String type = tupleType.f0;
                        //b. 获取窗口大小，开始时间和结束时间
                        long start = window.getStart();
                        long end = window.getEnd();
                        String winStart = this.format.format(start);
                        String winEnd = this.format.format(start);
                        //c. 遍历窗口数据进行累加
                        BigDecimal total = new BigDecimal(0.0).setScale(2, BigDecimal.ROUND_HALF_UP);
                        for (Tuple2<String, BigDecimal> item : input) {
                            total.add(item.f1);
                        }
                        double totalMount = total.doubleValue();
                        //d. 封装成 OrderReport 对象输出
                        OrderReport orderReport = new OrderReport(winStart, winEnd, type, totalMount);
                        out.collect(orderReport);
                    }
                });
        return resultStream;
    }


    /**
     * 6.实时报表统计：每日各省份销售额
     *
     * @param stream 清洗过后的数据流
     * @return 返回一个二元组，包含统计类型和总金额
     */


    /**
     * 7.实时报表统计：每日重点城市销售额
     *
     * @param stream 清洗过后的数据流
     * @return 返回一个二元组，包含统计类型和总金额
     */


    /**
     * 8.自定义 Sink，将统计数据保存的 MySQL
     *
     * @param stream 需要保存的数据流
     * @param table  保存的目标表
     * @param fields 需要保存的字段
     */
    private static void jdbcSink(DataStream<Tuple2<String, BigDecimal>> stream, String table, String fields) {
        //a. 构建Sink对象，设置属性
        SinkFunction<Tuple2<String, BigDecimal>> jdbcSink = JdbcSink.sink("replace into db_flink." + table + "(" + fields + ") values (?, ?)",//
                new JdbcStatementBuilder<Tuple2<String, BigDecimal>>() {
                    @Override
                    public void accept(PreparedStatement pstat, Tuple2<String, BigDecimal> value) throws SQLException {
                        pstat.setString(1, value.f0);
                        pstat.setDouble(2, value.f1.doubleValue());
                    }
                },//b. 设置批量写入时的参数值，批量大小，实时写入时设置为 1
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)//每批次大小，默认是5000.
                        .withBatchIntervalMs(0)//每批提交间隔
                        .withMaxRetries(3)//失败最大重试次数
                        .build(),//
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUrl("jdbc:mysql://node3:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        //c. 添加数据流到 Sink
        stream.addSink(jdbcSink);
    }

    //RUN AND DEBUG
    public static void main(String[] args) throws Exception {
        //1.准备环境-env,设置时间语义：事件时间
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //2.准备数据-source，从 kafka 加载数据
        DataStream<String> kafkaStream = kafkaSource(env, "orderTopic");
        //kafkaStream.printToErr();

        //3.处理数据-transformation
        //3.1 解析数据提前相关字段
        DataStream<OrderData> orderStream = streamETL(kafkaStream);
        orderStream.printToErr();
        //tupleStream.printToErr();
        //3.2 总销售额
        //DataStream<OrderReport> globalStream = reportDailyGlobal(orderDataStream);
        //3.3 各省份销售额
        // DataStream<Tuple2<String, BigDecimal>> provinceStream = reportProvince(tupleStream);
        //3.4 重点城市销售额
        //DataStream<Tuple2<String, BigDecimal>> cityStream = reportCity(tupleStream);

        //4.输出结果-sink，保存到 MYSQL 数据库
        //jdbcSink(globalStream, "tbl_report_global", "global, amount");
        //jdbcSink(provinceStream, "tbl_report_province", "province, amount");
        //jdbcSink(cityStream, "tbl_report_city", "city, amount");
        //globalStream.printToErr();

        //5.触发执行-execute
        env.execute(_07_02RealTimeDailyOrderReport.class.getName());

    }

}
