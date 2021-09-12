package com.test.flink.start

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @Author: Jface
 * @Date: 2021/9/5 17:57
 * @Desc: Scala 实现 Flink 读取文本数据做 wordcount
 */
object _02FlinkWordCount {
  def main(args: Array[String]): Unit = {
    //1.准备环境-env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //2.准备数据-source
    val inputDataSet: DataSet[String] = env.readTextFile("datas/wc.input")

    //3.处理数据-transformation
    //3.1 fiter 过滤
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .filter(x => null != x && x.trim.length > 0)
      //3.1 split 切割,需要导包
      .flatMap(x => x.trim.split("\\s+"))
      //3.2 map 转换成元组
      .map((_, 1))
      //3.3 分组聚合
      .groupBy(0).sum(1)
      //3.4 排序，全局排序需要设置分区数为 1
      .sortPartition(1, Order.DESCENDING)
      .setParallelism(1)
    //4.输出结果-sink
    resultDataSet.print()

    //5.触发执行-execute,没有写出，不需要触发

  }

}
