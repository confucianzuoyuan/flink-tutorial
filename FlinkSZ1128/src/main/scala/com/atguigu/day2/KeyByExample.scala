package com.atguigu.day2

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object KeyByExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream  = env
      .addSource(new SensorSource) // --> DataStream[T]
      .keyBy(r => r.id) // --> KeyedStream[T, K]
      .min(2)  // --> DataStream[T]

    stream.print()
    env.execute()
  }
}