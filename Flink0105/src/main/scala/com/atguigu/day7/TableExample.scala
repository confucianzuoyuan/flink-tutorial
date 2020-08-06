package com.atguigu.day7

import com.atguigu.day2.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

object TableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 有关表环境的配置
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // 使用blink planner，blink planner是流批统一
      .inStreamingMode()
      .build()

    // 初始化一个表环境
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/flink-tutorial/Flink0105/src/main/resources/sensor.txt")
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    tEnv.createTemporaryView("inputTable", stream)

    // 将临时表转换成Table数据类型
    val sensorTable: Table = tEnv.from("inputTable")

    // 使用Table API进行查询
    val result = sensorTable
      .select($"id", $"temperature")
      .filter($"id" === "sensor_1")

    tEnv.toAppendStream[Row](result).print()

    // 使用sql api进行查询
    val result1 = tEnv
      .sqlQuery("SELECT id, temperature FROM inputTable where id='sensor_1'")

    tEnv.toAppendStream[Row](result1).print()

    env.execute()
  }
}