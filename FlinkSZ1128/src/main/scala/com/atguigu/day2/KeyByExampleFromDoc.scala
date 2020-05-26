package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

object KeyByExampleFromDoc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    val resultStream: DataStream[(Int, Int, Int)] = inputStream
      .keyBy(0) // 使用元组的第一个元素进行分组
      .sum(1)   // 累加元组第二个元素

    resultStream.print()
    env.execute()
  }
}