package com.test.datastream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: Jface
 * @Date: 2021/10/17 17:35
 * @Desc: 使用Scala语言编写从TCP Socket读取数据，进行词频统计WordCount，结果打印至控制台。
 */
object _01WordCount {
  def main(args: Array[String]): Unit = {
    //1.Env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2);
    //2.Source
    val inputStream: DataStream[String] = env.socketTextStream("node1", 9999)

    //3.Transformation
    val resultStream: DataStream[(String, Int)] = inputStream.filter(x => null != x && x.trim.length > 0)
      .flatMap(x => x.trim.split("//s+"))
      .map((_, 1))
      .keyBy(0)
      .reduce((temp, item) => (temp._1, item._2 + temp._2))
    //4.Sink
    resultStream.print();
    //5.Execute
    env.execute(getClass.getSimpleName.stripSuffix("$"));
  }

}
