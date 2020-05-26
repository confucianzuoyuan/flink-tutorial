package com.atguigu.day8

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.functions.ScalarFunction

object ScalarFunctionExample {
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

    val hashCode = new HashCode(10)


    // 将流转换成动态表
    val dataTable = tableEnv
      .fromDataStream(stream, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    dataTable
        .select('id, hashCode('id))
        .toAppendStream[(String, Int)]
//        .print()


    // 注册udf函数
    tableEnv.registerFunction("hashCode", new HashCode(10))

//    tableEnv.createTemporaryView("t", dataTable, 'id)

    tableEnv
        .sqlQuery("SELECT id, hashCode(id) FROM " + dataTable)
        .toAppendStream[(String, Int)]
        .print()


    env.execute()
  }

  class HashCode(val factor: Int) extends ScalarFunction {
    def eval(s: String): Int = {
      s.hashCode() * factor
    }
  }
}