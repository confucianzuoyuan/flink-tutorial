package com.atguigu.day02

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .filter(r => r.id.equals("sensor_1"))

    stream
      .filter(new FilterFunction[SensorReading] {
        override def filter(t: SensorReading): Boolean = t.id.equals("sensor_1")
      })

    stream
      .filter(new MyFilter)
      .print()

    env.execute()
  }

  class MyFilter extends FilterFunction[SensorReading] {
    override def filter(t: SensorReading): Boolean = t.id.equals("sensor_1")
  }
}