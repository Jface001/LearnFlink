package com.test.flink.start

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Jface
 * @Date: 2021/9/7 17:45
 * @Desc: 使用Scala语言编写从TCP Socket读取数据，进行词频统计WordCount，结果打印至控制台。
 */
object _02FlinkWordCount {
  def main(args: Array[String]): Unit = {

    //1.准备环境-env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //2.准备数据-source
    val inputDataStream: DataStream[String] = env.socketTextStream("node1", 9999)
    //3.处理数据-transformation,需要导入 scala 包
    val resultDataStram: DataStream[(String, Int)] = inputDataStream
      //TODO: 过滤
      .filter(x => null != x && x.trim.length > 0)
      //TODO: 切割并扁平化
      .flatMap(line => line.trim.split("\\s+"))
      //TODO: 转换成元组
      .map((_, 1))
      //TODO: 分组
      .keyBy(0)
      //TODO: 聚合
      .reduce((temp, item) => (temp._1, temp._2 + item._2))
    //4.输出结果-sink
    resultDataStram.print()
    //5.触发执行-execute
    env.execute(this.getClass.getSimpleName.stripSuffix("$"))

  }

}
