package com.atguigu.day9

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object RetractStreamExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource)

    // sql
    tableEnv.createTemporaryView("sensor", stream)

    val result = tableEnv
      .sqlQuery("SELECT id, COUNT(id) FROM sensor WHERE id = 'sensor_1' GROUP BY id")

    tableEnv.toRetractStream[Row](result).print()

    // table api

    val table = tableEnv.fromDataStream(stream)

    val tableResult = table
      .filter($"id" === "sensor_1")
      .groupBy($"id")
      .select($"id", $"id".count())

    tableEnv.toRetractStream[Row](tableResult).print()


    env.execute()
  }
}