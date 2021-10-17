package com.test.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @Author: Jface
 * @Date: 2021/10/17 16:18
 * @Desc: Scala 开发 DataSet，实现 WordCount,记得导包
 */
object _01WordCount {
  def main(args: Array[String]): Unit = {
    //Env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //Source
    val dataSet: DataSet[String] = env.readTextFile("datas/wc.input")
    //Transformation，需要导包
    val ResultDataSet: DataSet[(String, Int)] = dataSet
      .filter(x => null != x && x.trim.length > 0)
      .flatMap(x => x.trim.split("\\s+"))
      .map(x => (x, 1))
      .groupBy(0)
      .sum(1)
      .setParallelism(1)
      .sortPartition(1, Order.DESCENDING)
    //Sink
    ResultDataSet.print();
    //Execute,批处理不需要
  }
}
