package com.atguigu.day11

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("hello#world", "atguigu#bigdata")

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.createTemporaryView("MyTable", stream, $"s")

    tEnv.createTemporarySystemFunction("SplitFunction", classOf[SplitFunction])

    // table api
    val tableResult = tEnv
      .from("MyTable")
      .joinLateral(call("SplitFunction", $"s"))
      .select($"s", $"word", $"length")

    tEnv.toAppendStream[Row](tableResult).print()

    // sql
    val sqlResult = tEnv.sqlQuery("SELECT s, word, length FROM MyTable, LATERAL TABLE(SplitFunction(s))")
    // val sqlResult = tEnv.sqlQuery("SELECT s, word, length FROM MyTable LEFT JOIN LATERAL TABLE(SplitFunction(s)) ON TRUE")

    tEnv.toAppendStream[Row](sqlResult).print()

    env.execute()
  }

  @FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
  class SplitFunction extends TableFunction[Row] {
    def eval(string: String): Unit = {
      string.split("#").foreach(s => collect(Row.of(s, Int.box(s.length))))
    }
  }
}