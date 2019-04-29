package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

object KeyedStreamExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // DataStream => KeyedStream => DataStream

    val stream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_1"))

    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(r => r.id)

    val maxStream: DataStream[SensorReading] = keyedStream.max(2)

    maxStream.print()

    env.execute()

  }
}