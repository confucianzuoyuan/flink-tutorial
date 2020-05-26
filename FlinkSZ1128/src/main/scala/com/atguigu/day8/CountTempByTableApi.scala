package com.atguigu.day8

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}

object CountTempByTableApi {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    // 表相关代码
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 将流转换成动态表
    val dataTable = tableEnv
      .fromDataStream(stream, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)
      .window(Tumble over 10.seconds on 'ts as 'w)
      .groupBy('id, 'w) // keyby.timeWindow
      .select('id, 'id.count) // 每个窗口有多少条数据

    // 将动态表转换成流
    dataTable
      .toRetractStream[(String, Long)] // `id, id.count`; 撤回流
      .print()

    env.execute()
  }
}