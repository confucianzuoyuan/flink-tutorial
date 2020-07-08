package com.atguigu.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapImplementMapAndFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    // 使用flatMap实现map功能，将传感器数据的id抽取出来
    stream
        .flatMap(new FlatMapFunction[SensorReading, String] {
          override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
            out.collect(value.id)
          }
        })
        .print()

    // 使用flatMap实现filter功能, 将sensor_1的数据过滤出来
    stream
        .flatMap(new FlatMapFunction[SensorReading, SensorReading] {
          override def flatMap(value: SensorReading, out: Collector[SensorReading]): Unit = {
            if (value.id.equals("sensor_1")) {
              out.collect(value)
            }
          }
        })
        .print()

    env.execute()
  }
}