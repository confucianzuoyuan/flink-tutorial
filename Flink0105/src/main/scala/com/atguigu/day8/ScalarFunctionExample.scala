package com.atguigu.day8

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings
        .newInstance()
        .inStreamingMode()
        .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val hashCode = new HashCode(10)

    // table写法
    val table = tEnv.fromDataStream(stream)

    table
        .select('id, hashCode('id))
        .toAppendStream[Row]
//        .print()

    // sql 写法
    tEnv.registerFunction("hashCode", hashCode)

    tEnv.createTemporaryView("sensor", table)

    tEnv
        .sqlQuery("SELECT id, hashCode(id) FROM sensor")
        .toAppendStream[Row]
        .print()

    env.execute()
  }

  class HashCode(factor: Int) extends ScalarFunction {
    def eval(s: String): Int = {
      s.hashCode * factor
    }
  }
}