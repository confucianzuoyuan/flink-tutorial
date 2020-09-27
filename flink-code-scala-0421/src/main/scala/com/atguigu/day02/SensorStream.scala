package com.atguigu.day02

import com.atguigu.day02.util.SensorSource
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