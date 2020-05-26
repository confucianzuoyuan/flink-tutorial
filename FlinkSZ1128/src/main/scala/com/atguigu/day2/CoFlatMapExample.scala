package com.atguigu.day2

import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    println(env.getParallelism) // 打印默认并行度

    val one: DataStream[(Int, Long)] = env
      .fromElements((1, 1L))
      .setParallelism(1)
    val two: DataStream[(Int, String)] = env
      .fromElements((1, "two"))
      .setParallelism(1)

    // 将key相同的联合到一起
    val connected: ConnectedStreams[(Int, Long), (Int, String)] = one.keyBy(_._1)
      .connect(two.keyBy(_._1))

    val printed: DataStream[String] = connected
      .flatMap(new MyCoFlatMap)

    printed.print

    env.execute()
  }

  class MyCoFlatMap extends CoFlatMapFunction[(Int, Long), (Int, String), String] {
    override def flatMap1(value: (Int, Long), out: Collector[String]): Unit = {
      out.collect(value._2.toString + "来自第一条流")
      out.collect(value._2.toString + "来自第一条流")
    }

    override def flatMap2(value: (Int, String), out: Collector[String]): Unit = {
      out.collect(value._2 + "来自第二条流")
    }
  }

}