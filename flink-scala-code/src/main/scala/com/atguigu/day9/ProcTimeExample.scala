package com.atguigu.day9

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object ProcTimeExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource)

    val tableProcTime = tableEnv.fromDataStream(stream, $"id", $"timestamp", $"temperature", $"pt".proctime())

    val tableEventTime = tableEnv.fromDataStream(stream, $"id", $"timestamp".rowtime() as "ts")

    val tableResult = tableProcTime
      .select($"id", $"timestamp", $"pt")

    tableEnv.toAppendStream[Row](tableResult).print()

    env.execute()
  }
}