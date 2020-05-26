package com.atguigu.day2

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val one: DataStream[(Int, Long)] = env.fromElements((1, 1L))
    val two: DataStream[(Int, String)] = env.fromElements((1, "two"))

    // 将key相同的联合到一起
    val connected: ConnectedStreams[(Int, Long), (Int, String)] = one.keyBy(_._1)
      .connect(two.keyBy(_._1))

    val printed: DataStream[String] = connected
      .map(new MyCoMap)

    printed.print

    env.execute()
  }

  class MyCoMap extends CoMapFunction[(Int, Long), (Int, String), String] {
    override def map1(value: (Int, Long)): String = value._2.toString + "来自第一条流"

    override def map2(value: (Int, String)): String = value._2 + "来自第二条流"
  }
}