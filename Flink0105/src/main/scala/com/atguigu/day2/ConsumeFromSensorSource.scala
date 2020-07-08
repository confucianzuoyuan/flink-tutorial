package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

object ConsumeFromSensorSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 调用addSource方法
    val stream : DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.print()

    env.execute()
  }
}