package com.atguigu.course

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction

object TableUDFExample2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val split = new Split("#")
    env.setParallelism(1)
    val stream = env.fromElements(
      "hello#world"
    )
    val table = tEnv.fromDataStream(stream, 's)
    // table api 写法
    table
      .joinLateral(split('s) as ('word, 'length))
      .select('s, 'word, 'length)
      .toAppendStream[(String, String, Long)]
      .print()
    table
      .leftOuterJoinLateral(split('s) as ('word, 'length))
      .select('s, 'word, 'length)
      .toAppendStream[(String, String, Long)]
      .print()

    // sql 写法
    tEnv.registerFunction("split", new Split("#"))

    tEnv.createTemporaryView("t", table, 's)

    // Use the table function in SQL with LATERAL and TABLE keywords.
    // CROSS JOIN a table function (equivalent to "join" in Table API)
    tEnv
      .sqlQuery("SELECT s, word, length FROM t, LATERAL TABLE(split(s)) as T(word, length)")
      .toAppendStream[(String, String, Int)]
      .print()
    // LEFT JOIN a table function (equivalent to "leftOuterJoin" in Table API)
    tEnv
      .sqlQuery("SELECT s, word, length FROM t LEFT JOIN LATERAL TABLE(split(s)) as T(word, length) ON TRUE")
      .toAppendStream[(String, String, Int)]
      .print()

    env.execute()

  }
  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      // use collect(...) to emit a row.
      str.split(separator).foreach(x => collect((x, x.length)))
    }
  }
}