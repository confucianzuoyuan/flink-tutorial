package com.atguigu.day8

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "hello#world",
        "atguigu#bigdata"
      )

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    // table 写法
    val table = tEnv.fromDataStream(stream, 's)

    val split = new Split("#")

    table
      // 为了将`hello#world`和`hello 5`join到一行
      .joinLateral(split('s) as ('word, 'length))
      // 下面的写法和上面的写法等价
      // .leftOuterJoinLateral(split('s) as ('word, 'length))
      .select('s, 'word, 'length)
      .toAppendStream[Row]
//      .print()

    // sql 写法
    tEnv.registerFunction("split", split)

    tEnv.createTemporaryView("t", table)

    tEnv
      // `T`的意思是元组，flink里面的固定语法
        .sqlQuery("SELECT s, word, length FROM t, LATERAL TABLE(split(s)) as T(word, length)")
      //.sqlQuery("SELECT s, word, length FROM t LEFT JOIN LATERAL TABLE(split(s)) as T(word, length) ON TRUE")
        .toAppendStream[Row]
        .print()

    env.execute()
  }

  // 输出的泛型是(String, Int)
  class Split(sep: String) extends TableFunction[(String, Int)] {
    def eval(s: String): Unit = {
      // 使用collect方法向下游发送数据
      s.split(sep).foreach(x => collect((x, x.length)))
    }
  }
}