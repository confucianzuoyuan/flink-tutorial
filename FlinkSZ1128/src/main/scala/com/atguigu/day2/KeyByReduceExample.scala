package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

object KeyByReduceExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      // 第二个字段时间戳为定义
      .reduce((r1, r2) => SensorReading(r1.id, 0L, r1.temperature.min(r2.temperature)))

    stream.print()

    env.execute()
  }
}