package com.atguigu.day2

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(1,2,3)
    stream
      .flatMap(new MyFlatMap)
      .print()

    env.execute()
  }

  class MyFlatMap extends RichFlatMapFunction[Int, Int] {
    override def open(parameters: Configuration): Unit = {
      println("开始生命周期")
    }

    override def flatMap(value: Int, out: Collector[Int]): Unit = {
      println("并行任务索引是：" + getRuntimeContext.getIndexOfThisSubtask)
      out.collect(value+1)
    }

    override def close(): Unit = {
      println("结束生命周期")
    }
  }
}