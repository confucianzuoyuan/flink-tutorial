package com.atguigu.day3

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object MinTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))
      .print()

    env.execute()
  }
}