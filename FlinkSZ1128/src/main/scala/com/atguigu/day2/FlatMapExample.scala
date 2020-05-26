package com.atguigu.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    // 使用`FlatMapFunction`实现`MapExample.scala`中的功能
    stream
      .flatMap(
        new FlatMapFunction[SensorReading, String] {
          override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
            // 使用`collect`方法向下游发送抽取的传感器ID
            out.collect(value.id)
          }
        }
      )
      .print()

    // 使用`FlatMapFunction`实现`FilterExample.scala`中的功能
    stream
        .flatMap(
          new FlatMapFunction[SensorReading, SensorReading] {
            override def flatMap(value: SensorReading, out: Collector[SensorReading]): Unit = {
              if (value.id.equals("sensor_1")) {
                out.collect(value)
              }
            }
          }
        )
        .print()

    env.execute()
  }
}