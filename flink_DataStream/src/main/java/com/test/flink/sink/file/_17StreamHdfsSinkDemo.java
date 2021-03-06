package com.test.flink.sink.file;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Flink Stream 流计算，将DataStream 保存至文件系统，使用FileSystem Connector
 *  自定义桶名称
 */
public class _17StreamHdfsSinkDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 1-1. 设置并行度为3
		env.setParallelism(3);
		// 1-2. 设置Checkpoint时间间隔
		env.enableCheckpointing(5000);

		// 2. 数据源-source
		DataStreamSource<String> orderDataStream = env.addSource(new ParallelSourceFunction<String>() {
			private boolean isRunning = true ;
			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				Random random = new Random();
				while (isRunning){
					// 交易订单
					String orderId = "o_" + UUID.randomUUID().toString().substring(0, 8) ;
					String userId = "u-" + (10000 + random.nextInt(10000)) ;
					Double orderMoney = new BigDecimal(random.nextDouble() * 100).setScale(2, RoundingMode.HALF_UP).doubleValue() ;
					String output = orderId + "," + userId + "," + orderMoney + "," + System.currentTimeMillis() ;
					// 输出
					ctx.collect(output);
					TimeUnit.MILLISECONDS.sleep(100);
				}
			}

			@Override
			public void cancel() {
				isRunning = false ;
			}
		});
		//orderDataStream.print();

		// 3. 数据转换-transformation
		// 4. 数据终端-sink
		// 创建FileSink对象，进行设置相关属性
		StreamingFileSink<String> sink = StreamingFileSink
			// 4-1. 设置问价存储格式，使用行式存储
			.forRowFormat(new Path("datas/file-sink"), new SimpleStringEncoder<String>())
			// 4-2. 设置滚动策略
			.withRollingPolicy(
				DefaultRollingPolicy.builder()
					.withRolloverInterval(TimeUnit.MINUTES.toMillis(1)) // 多长时间滚动一次文件
					.withInactivityInterval(TimeUnit.SECONDS.toMillis(30)) // 多久不写入滚动一次文件
					.withMaxPartSize(2 * 1024 * 1024) // 多大滚动一次文件
					.build()
			)
			// 4-3. 桶的分配器，默认按照yyyy-MM-dd--HH产生桶
			.withBucketAssigner(new DayBucketAssigner())
			// 4-4. 设置输出文件的名称
			.withOutputFileConfig(
				new OutputFileConfig("order", ".test")
			)
			.build();
		// 4-5. 为数据流添加Sink输出对象
		orderDataStream.addSink(sink) ;

		// 5. 触发执行
		env.execute(_17StreamHdfsSinkDemo.class.getSimpleName());
	}

	/*
		桶分配器接口：BucketAssigner<IN, BucketID>
				IN-> 表示DataStream中数据类型，此处就是String
				BucketID -> 表示桶的ID（名称）数据类型
	 */
	private static class DayBucketAssigner implements BucketAssigner<String, String>{
		private FastDateFormat format = FastDateFormat.getInstance("yyyyMMdd");
		@Override
		public String getBucketId(String element, Context context) {
			// o_b87c9666,u-19128,37.26,1630999577695 提取日期字段
			String[] array = element.trim().split(",");
			String orderTime = array[3];
			// 格式日期格式
			String orderDate = this.format.format(Long.parseLong(orderTime));
			// 拼凑字符串
			String output = "dt=" + orderDate ;
			// 返回桶的名称
			return output;
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return  SimpleVersionedStringSerializer.INSTANCE;
		}
	}

}