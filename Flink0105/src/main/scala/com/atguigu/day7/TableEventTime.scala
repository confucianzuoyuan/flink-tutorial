package com.atguigu.day7

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object TableEventTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    // `.rowtime`指定已有字段为事件时间
    val table: Table = tEnv.fromDataStream(stream, $"id", $"timestamp".rowtime as "ts", $"temperature" as "temp")

    table
        .window(Slide over 10.seconds every 5.seconds on $"ts" as $"w")
        .groupBy($"id", $"w")
        .select($"id", $"id".count)
        .toAppendStream[Row]
        .print()

    env.execute()
  }
}