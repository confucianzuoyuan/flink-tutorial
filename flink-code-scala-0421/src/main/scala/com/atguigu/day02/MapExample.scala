package com.atguigu.day02

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .map(r => r.id)
      .print()

    stream
      .map(new MapFunction[SensorReading, String] {
        override def map(t: SensorReading): String = t.id
      })
      .print()

    stream
      .map(new MyMap)
      .print()

    env.execute()
  }

  class MyMap extends MapFunction[SensorReading, String] {
    override def map(t: SensorReading): String = t.id
  }
}