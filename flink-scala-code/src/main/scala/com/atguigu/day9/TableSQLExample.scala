package com.atguigu.day9

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

// taStream => Table/Temporary View => Table API/SQL => Table => DataStream
object TableSQLExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource)

    // DataStream => Table
    // table api
    val table: Table = tableEnv.fromDataStream(stream)
    // or can be written as:
    // val table = ableEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature" as "temp")

    val tableResult = table
      .filter($"id" === "sensor_1")
      .select($"id", $"temperature") // $"temp"

    tableEnv
      .toAppendStream[Row](tableResult)
      .print()

    // sql

    tableEnv.createTemporaryView("sensor", stream)
    // tableEnv.createTemporaryView("sensor", stream, $"id", $"timestamp" as "ts", $"temperature" as "temp")

    val sqlResult = tableEnv
      .sqlQuery("SELECT * FROM sensor where id = 'sensor_2'")

    tableEnv
      // T=> DataStream
      .toAppendStream[Row](sqlResult)
      .print()

    env.execute()
  }
}