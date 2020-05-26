package com.atguigu.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExampleFromDoc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "white", "black", "white", "gray", "black", "white"
      )

    stream
      .flatMap(
        new FlatMapFunction[String, String] {
          override def flatMap(value: String, out: Collector[String]): Unit = {
            if (value.equals("white")) {
              out.collect(value)
            } else if (value.equals("black")) {
              out.collect(value)
              out.collect(value)
            }
          }
        }
      )
      .print()

    env.execute()
  }
}