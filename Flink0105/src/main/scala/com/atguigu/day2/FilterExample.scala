package com.atguigu.day2

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.filter(r => r.id.equals("sensor_1")).print()

    stream.filter(new FilterFunction[SensorReading] {
      override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
    })

    stream.filter(new MyFilterFunction).print()

    env.execute()
  }

  // filter算子输入和输出的类型是一样的，所以只有一个泛型SensorReading
  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
  }
}