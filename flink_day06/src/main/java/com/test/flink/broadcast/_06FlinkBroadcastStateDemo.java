package com.test.flink.broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息
 *      TODO: 用户信息存储在MySQL数据库表
 *  实时将大表与小表数据进行关联，其中小表数据动态变化
 *      大表数据：流式数据，存储Kafka消息队列，此处演示自定义数据源产生日志流数据
 *      小表数据：动态数据，存储MySQL数据库
 *   TODO： BroadcastState 将小表数据进行广播，封装到Map集合集合中，使用connect函数与大表数据流进行连接
 */
public class _06FlinkBroadcastStateDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		// 2-1. 构建大表数据流: 用户行为日志，<userId, productId, trackTime, eventType>
		DataStreamSource<TrackLog> trackLogSource = env.addSource(new TrackLogSource());
		// 2-2. 构建小表数据流: 用户信息，<userId, name, age>
		DataStreamSource<UserInfo> userInfoSource = env.addSource(new UserInfoSource());

		// 3. 数据转换-transformation
		/*
			将小表数据流，广播的以后，将其存储到MapState中，方便大表数据流数据处理时使用
				Map[userId, userInfo]
			大表中数据处理，依据userId，获取小表中对应用的信息UserInfo
				map.get(userId)  -> userIfo
		 */
		// 3-1. 先将小表数据流进行广播
		MapStateDescriptor<String, UserInfo> desc = new MapStateDescriptor<>("userMap", String.class, UserInfo.class);
		BroadcastStream<UserInfo> broadcastStream = userInfoSource.broadcast(desc);

		// 3-2. 将大表数据流，要与小表数据流关联
		SingleOutputStreamOperator<String> processStream = trackLogSource
				// 与小表广播后数据流关联
				.connect(broadcastStream)
				// 对流中数据进行处理，包含大表数据流和小表数据流
				.process(new BroadcastProcessFunction<TrackLog, UserInfo, String>() {
					//大表中的每一条数据
					@Override
					public void processElement(TrackLog value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
						//获取广播状态，就是小表数据
						ReadOnlyBroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(desc);
						//根据 userId 获取小表中的数据
						UserInfo userInfo = broadcastState.get(value.getUserId());
						//拼接输出
						String output = value + "<->" + userInfo;
						out.collect(output);


					}

					//小表中的每一条数据
					@Override
					public void processBroadcastElement(UserInfo value, Context ctx, Collector<String> out) throws Exception {
						//将小表中的每一条数据存储到 broadcastState 中，方便大表获取
						BroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(desc);
						broadcastState.put(value.getUserId(), value);
					}
				});


		// 4. 数据终端-sink
		processStream.printToErr();


		// 5. 触发执行-execute
		env.execute("BroadcastStateDemo");
	}

}



