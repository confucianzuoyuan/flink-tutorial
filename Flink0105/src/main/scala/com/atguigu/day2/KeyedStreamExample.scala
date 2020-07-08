package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

object KeyedStreamExample {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream : DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))

    // 泛型变成了两个，第二个泛型是key的类型
    val keyed : KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    // 使用第三个字段来做滚动聚合，求每个传感器流上的最小温度值
    // 内部会保存一个最小值的状态变量，用来保存到来的温度的最小值
    // 滚动聚合以后，流的类型又变成了`DataStream`
    val min : DataStream[SensorReading] = keyed.min(2)
    min.print()

    // reduce也会保存一个状态变量
    val red : DataStream[SensorReading] = keyed.reduce((r1, r2) => SensorReading(r1.id, 0L, r1.temperature.min(r2.temperature)))
    red.print()

    env.execute()
  }
}