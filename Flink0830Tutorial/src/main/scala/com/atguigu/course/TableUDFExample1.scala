package com.atguigu.course

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction

object TableUDFExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    val hashCode = new HashCode(10)
    tEnv.registerFunction("hashCode", new HashCode(10))
    val table = tEnv.fromDataStream(stream, 'id)
    // table api 写法
    table
      .select('id, hashCode('id))
      .toAppendStream[(String, Int)]
      .print()

    // sql 写法
    tEnv.createTemporaryView("t", table, 'id)
    tEnv
      .sqlQuery("SELECT id, hashCode(id) FROM t")
      .toAppendStream[(String, Int)]
      .print()

    env.execute()
  }

  class HashCode(factor: Int) extends ScalarFunction {
    def eval(s: String): Int = {
      s.hashCode() * factor
    }
  }

}