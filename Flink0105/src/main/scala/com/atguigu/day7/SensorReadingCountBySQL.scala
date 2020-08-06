package com.atguigu.day7

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object SensorReadingCountBySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))

    tEnv.createTemporaryView("sensor", stream, $"id", $"timestamp" as "ts", $"temperature")

    tEnv
      .sqlQuery("SELECT id, count(id) FROM sensor GROUP BY id")
      .toRetractStream[Row]
      .print()

    env.execute()
  }
}