package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

object SensorStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.print()

    env.execute()
  }
}