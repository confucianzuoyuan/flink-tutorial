package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

object UnionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 传感器ID为sensor_1的数据为来自巴黎的流
    val parisStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_1"))

    // 传感器ID为sensor_2的数据为来自东京的流
    val tokyoStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_2"))

    // 传感器ID为sensor_3的数据为来自里约的流
    val rioStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_3"))

    val allCities: DataStream[SensorReading] = parisStream
      .union(
        tokyoStream,
        rioStream
      )

    allCities.print()

    env.execute()
  }
}