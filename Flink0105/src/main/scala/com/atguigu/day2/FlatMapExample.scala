package com.atguigu.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("white", "gray", "black")

    // `flatMap`针对流中的每一个元素，生成0个，1个，或者多个数据
    stream
        .flatMap(new MyFlatMapFunction)
        .print()

    env.execute()
  }

  class MyFlatMapFunction extends FlatMapFunction[String, String] {
    // 调用`out.collect`方法，将数据发送到下游
    override def flatMap(value: String, out: Collector[String]): Unit = {
      if (value.equals("white")) {
        out.collect(value)
      } else if (value.equals("black")) {
        out.collect(value)
        out.collect(value)
      }
    }
  }
}