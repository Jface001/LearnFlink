package com.test.flink.order.report;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
public class _07RealTimeOrderReport {
    /**
     * 1.自定义 JavaBean 对象，用于解析 JSON 字符串
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class OrderData {
        String orderId;
        String userId;
        String orderTime;
        String ip;
        Double orderMoney;
        Integer orderStatus;
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
     * @return 返回一个三元组，省份，城市，总额
     */
    private static DataStream<Tuple3<String, String, BigDecimal>> streamETL(DataStream<String> stream) {
        SingleOutputStreamOperator<Tuple3<String, String, BigDecimal>> resultStream = stream
                //a. 解析JSON数据，封装实体类对象
                .map(new RichMapFunction<String, OrderData>() {
                    @Override
                    public OrderData map(String value) throws Exception {
                        return JSON.parseObject(value, OrderData.class);
                    }
                })
                //b. 过滤订单状态为0的数据
                .filter(new RichFilterFunction<OrderData>() {
                    @Override
                    public boolean filter(OrderData order) throws Exception {
                        return 0 == order.orderStatus;
                    }
                })
                //c. 解析IP地址为省份和城市
                .map(new RichMapFunction<OrderData, Tuple3<String, String, BigDecimal>>() {
                    //定义变量，用于解析 IP 地址为省份和城市
                    private DbSearcher dbSearcher = null;

                    //初始化 dbSearcher 对象，用于解析IP 获取省份和城市
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dbSearcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db");
                    }

                    @Override
                    public Tuple3<String, String, BigDecimal> map(OrderData order) throws Exception {
                        // 获取 ip
                        String ipValue = order.ip;
                        // 解析 ip ，获取省份和城市
                        DataBlock dataBlock = dbSearcher.btreeSearch(ipValue);
                        String[] arr = dataBlock.getRegion().split("\\|");
                        String province = arr[2];
                        String city = arr[3];
                        // 获取订单金额并转换 BigDecimal 数据类型，方便累加计算，防止精度损失
                        Double orderMoney = order.orderMoney;
                        BigDecimal money = new BigDecimal(orderMoney).setScale(2, BigDecimal.ROUND_HALF_UP);
                        return Tuple3.of(province, city, money);
                    }
                });

        //返回消费后的结果流
        return resultStream;
    }

    ;

    /**
     * 5.实时报表统计：每日总销售额
     *
     * @param stream 清洗过后的数据流
     * @return 返回一个二元组，包含统计类型和总金额
     */
    private static DataStream<Tuple2<String, BigDecimal>> reportGlobal(DataStream<Tuple3<String, String, BigDecimal>> stream) {
        SingleOutputStreamOperator<Tuple2<String, BigDecimal>> resultStream = stream
                //a. 提取字段，增加一个字段：全国
                .map(new RichMapFunction<Tuple3<String, String, BigDecimal>, Tuple2<String, BigDecimal>>() {
                    @Override
                    public Tuple2<String, BigDecimal> map(Tuple3<String, String, BigDecimal> value) throws Exception {
                        return Tuple2.of("全国", value.f2);
                    }
                })
                //b. 按照全国分组
                .keyBy(0)
                //c. 组内数据求和，Decimal 类型 不能使用 sum
                .reduce(new RichReduceFunction<Tuple2<String, BigDecimal>>() {
                    @Override
                    public Tuple2<String, BigDecimal> reduce(Tuple2<String, BigDecimal> temp, Tuple2<String, BigDecimal> item) throws Exception {
                        String key = temp.f0;
                        BigDecimal total = temp.f1.add(item.f1);
                        return Tuple2.of(key, total);
                    }
                });
        //d. 返回最新销售额
        return resultStream;
    }

    /**
     * 6.实时报表统计：每日各省份销售额
     *
     * @param stream 清洗过后的数据流
     * @return 返回一个二元组，包含统计类型和总金额
     */
    private static DataStream<Tuple2<String, BigDecimal>> reportProvince(DataStream<Tuple3<String, String, BigDecimal>> stream) {
        SingleOutputStreamOperator<Tuple2<String, BigDecimal>> resultStream = stream
                //a. 提取字段
                .map(new RichMapFunction<Tuple3<String, String, BigDecimal>, Tuple2<String, BigDecimal>>() {
                    @Override
                    public Tuple2<String, BigDecimal> map(Tuple3<String, String, BigDecimal> value) throws Exception {
                        return Tuple2.of(value.f0, value.f2);
                    }
                })
                //b. 按照省份分组
                .keyBy(0)
                //c. 组内数据求和，Decimal 类型 不能使用 sum
                .reduce(new RichReduceFunction<Tuple2<String, BigDecimal>>() {
                    @Override
                    public Tuple2<String, BigDecimal> reduce(Tuple2<String, BigDecimal> temp, Tuple2<String, BigDecimal> item) throws Exception {
                        String key = temp.f0;
                        BigDecimal total = temp.f1.add(item.f1);
                        return Tuple2.of(key, total);
                    }
                });
        //d. 返回最新销售额
        return resultStream;
    }

    ;

    /**
     * 7.实时报表统计：每日重点城市销售额
     *
     * @param stream 清洗过后的数据流
     * @return 返回一个二元组，包含统计类型和总金额
     */
    private static DataStream<Tuple2<String, BigDecimal>> reportCity(DataStream<Tuple3<String, String, BigDecimal>> stream) {

        List<String> cityList = new ArrayList<String>();
        cityList.add("北京市");
        cityList.add("上海市");
        cityList.add("深圳市");
        cityList.add("广州市");
        cityList.add("杭州市");
        cityList.add("成都市");
        cityList.add("南京市");
        cityList.add("武汉市");
        cityList.add("西安市");
        SingleOutputStreamOperator<Tuple2<String, BigDecimal>> resultStream =
                stream
                        //过滤，只提取重点城市
                        .filter(new RichFilterFunction<Tuple3<String, String, BigDecimal>>() {
                            @Override
                            public boolean filter(Tuple3<String, String, BigDecimal> value) throws Exception {
                                return cityList.contains(value.f1);
                            }
                        })
                        //a. 提取字段
                        .map(new RichMapFunction<Tuple3<String, String, BigDecimal>, Tuple2<String, BigDecimal>>() {
                            @Override
                            public Tuple2<String, BigDecimal> map(Tuple3<String, String, BigDecimal> value) throws Exception {
                                return Tuple2.of(value.f1, value.f2);
                            }
                        })
                        //b. 按照城市分组
                        .keyBy(0)
                        //c. 组内数据求和，Decimal 类型 不能使用 sum
                        .reduce(new RichReduceFunction<Tuple2<String, BigDecimal>>() {
                            @Override
                            public Tuple2<String, BigDecimal> reduce(Tuple2<String, BigDecimal> temp, Tuple2<String, BigDecimal> item) throws Exception {
                                String key = temp.f0;
                                BigDecimal total = temp.f1.add(item.f1);
                                return Tuple2.of(key, total);
                            }
                        });
        //d. 返回最新销售额
        return resultStream;


    }

    ;

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
        //1.准备环境-env,设置 Checkpoint 属性
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        setEnvCheckPoint(env);

        //2.准备数据-source，从 kafka 加载数据
        DataStream<String> kafkaStream = kafkaSource(env, "orderTopic");
        //kafkaStream.printToErr();

        //3.处理数据-transformation
        //3.1 解析数据提前相关字段
        DataStream<Tuple3<String, String, BigDecimal>> tupleStream = streamETL(kafkaStream);
        //tupleStream.printToErr();
        //3.2 总销售额
        DataStream<Tuple2<String, BigDecimal>> globalStream = reportGlobal(tupleStream);
        //3.3 各省份销售额
        DataStream<Tuple2<String, BigDecimal>> provinceStream = reportProvince(tupleStream);
        //3.4 重点城市销售额
        DataStream<Tuple2<String, BigDecimal>> cityStream = reportCity(tupleStream);

        //4.输出结果-sink，保存到 MYSQL 数据库
        //globalStream.printToErr();
        //provinceStream.printToErr();
        //cityStream.printToErr();
        jdbcSink(globalStream, "tbl_report_global", "global, amount");
        jdbcSink(provinceStream, "tbl_report_province", "province, amount");
        jdbcSink(cityStream, "tbl_report_city", "city, amount");

        //5.触发执行-execute
        env.execute(_07RealTimeOrderReport.class.getName());

    }

}
