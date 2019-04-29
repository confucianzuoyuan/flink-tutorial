package com.atguigu.day3

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(1,2,3)

    stream
      .map(new MyMapFunction)
      .print()
    env.execute()
  }

  class MyMapFunction extends RichMapFunction[Int, Int] {
    override def open(parameters: Configuration): Unit = {
      println("enter into lifecycle")
    }

    override def map(value: Int): Int = {
      value + 1
    }

    override def close(): Unit = {
      println("exit lifecycle")
    }
  }
}