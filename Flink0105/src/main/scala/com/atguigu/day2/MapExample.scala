package com.atguigu.day2

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream : DataStream[SensorReading] = env.addSource(new SensorSource)

    val mapped1 : DataStream[String] = stream.map(r => r.id)

    val mapped2 : DataStream[String] = stream.map(new MyMapFunction)

    val mapped3 : DataStream[String] = stream
      .map(new MapFunction[SensorReading, String] {
        override def map(value: SensorReading): String = value.id
      })

    mapped1.print()
    mapped2.print()
    mapped3.print()

    env.execute()
  }

  // 输入泛型：SensorReading; 输出泛型：String;
  class MyMapFunction extends MapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = value.id
  }
}