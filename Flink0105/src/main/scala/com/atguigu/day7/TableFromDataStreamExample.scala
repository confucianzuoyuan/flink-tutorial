package com.atguigu.day7

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object TableFromDataStreamExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    // 字段名必须以`'`开始，as用来取别名
    // DataStream => Table
    val table: Table = tEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature")

    table
      .select($"id")
      .toAppendStream[Row] // 追加流
      .print()

    // 使用数据流来创建名字为sensor的临时表
    // 创建临时表的目的是为了在临时表上做sql查询
    tEnv.createTemporaryView("sensor", stream, $"id", $"timestamp" as "ts", $"temperature")

    tEnv
        .sqlQuery("select * from sensor where id='sensor_1'")
        .toRetractStream[Row] // 第一个字段true，表示追加，false表示撤回
        .print()

    // DataStream => Table(动态表) => CRUD => Table(动态表) => DataStream => Sink

    env.execute()
  }
}