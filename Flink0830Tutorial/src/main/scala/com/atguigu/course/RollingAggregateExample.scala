package com.atguigu.course

import org.apache.flink.streaming.api.scala._

// keyBy需要进行shuffle操作，宽依赖
object RollingAggregateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 滚动聚合的例子
    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    inputStream
      .keyBy(0)
      .sum(1)
//      .minBy(1)
      .print()

    env.execute()
  }
}
