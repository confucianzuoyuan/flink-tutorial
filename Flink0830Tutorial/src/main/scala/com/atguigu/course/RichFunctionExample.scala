package com.atguigu.course

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env
      .fromElements(1,2,3)
      .map(new MyRichMapFunction)
      .print()

    env.execute()
  }

  class MyRichMapFunction extends RichMapFunction[Int, Int] {
    override def open(parameters: Configuration): Unit = {
      println("进入map函数了！")
    }

    override def map(value: Int): Int = value + 1

    override def close(): Unit = {
      println("结束map函数了！")
    }
  }
}
