package com.atguigu.day8

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
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

    tEnv.getConfig.addJobParameter("hashcode_factor", "31")

    tEnv.createTemporaryView("sensor", stream)

    // 在 Table API 里不经注册直接“内联”调用函数
    tEnv.from("sensor").select(call(classOf[HashCodeFunction], $"id"))

    // sql 写法
    // 注册函数
    tEnv.createTemporarySystemFunction("hashCode", classOf[HashCodeFunction])

    // 在 Table API 里调用注册好的函数
    tEnv.from("sensor").select(call("hashCode", $"id"))

    tEnv
        .sqlQuery("SELECT id, hashCode(id) FROM sensor")
        .toAppendStream[Row]
        .print()

    env.execute()
  }

  class HashCodeFunction extends ScalarFunction {

    private var factor: Int = 0

    override def open(context: FunctionContext): Unit = {
      // 获取参数 "hashcode_factor"
      // 如果不存在，则使用默认值 "12"
      factor = context.getJobParameter("hashcode_factor", "12").toInt
    }

    def eval(s: String): Int = {
      s.hashCode * factor
    }
  }
}