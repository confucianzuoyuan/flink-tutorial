package com.atguigu.day8

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements("hello#world", "atguigu#zuoyuan")

    // 表相关代码
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val split = new Split("#")

    val dataTable = tableEnv.fromDataStream(stream, 's)

    dataTable
        // 为什么要进行join？因为要将s和split(s) join到一行
//        .joinLateral(split('s) as ('word, 'length))
      // 功能一样
        .leftOuterJoinLateral(split('s) as ('word, 'length))
        .select('s, 'word, 'length)
        .toAppendStream[(String, String, Int)]
//        .print()


    // 注册udf函数
    tableEnv.registerFunction("split", new Split("#"))
    tableEnv.createTemporaryView("t", dataTable)

    tableEnv
        // `T`是元组的意思
        .sqlQuery(
          """
            |SELECT s, word, length from
            | t
            | LEFT JOIN LATERAL TABLE(split(s)) AS T(word, length) ON TRUE""".stripMargin)
        .toAppendStream[Row]
        .print()

    env.execute()
  }

  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      str.split(separator).foreach(word => collect((word, word.length)))
    }
  }
}