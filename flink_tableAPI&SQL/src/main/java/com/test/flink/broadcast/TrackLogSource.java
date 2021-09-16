package com.test.flink.broadcast;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源，实时产生用户访问网站点击流数据
 */
public class TrackLogSource extends RichParallelSourceFunction<TrackLog> {
	private boolean isRunning = true ;

	@Override
	public void run(SourceContext<TrackLog> ctx) throws Exception {
		String[] types = new String[]{
			"click", "browser", "search", "click", "browser", "browser", "browser",
			"click", "search", "click", "browser", "click", "browser", "browser", "browser"
		} ;
		Random random = new Random() ;
		FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss:SSS") ;

		while (isRunning){
			TrackLog clickLog = new TrackLog(
				"user_" + (random.nextInt(4) + 1), //
				10000 + random.nextInt(10000), //
				format.format(System.currentTimeMillis()), //
				types[random.nextInt(types.length)]
			);
			ctx.collect(clickLog);

			// 每个1秒生成一条数据
			TimeUnit.MILLISECONDS.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isRunning = false ;
	}

}
