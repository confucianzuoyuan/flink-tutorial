package com.atguigu.day02

import com.atguigu.day02.util.SensorReading
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("white", "black", "gray")

    stream
      .flatMap(new FlatMapFunction[String, String] {
        override def flatMap(t: String, collector: Collector[String]): Unit = {
          if (t.equals("white")) {
            collector.collect(t)
          } else if (t.equals("black")) {
            collector.collect(t)
            collector.collect(t)
          }
        }
      })

    stream
      .flatMap(new MyFlatMap)
      .print()

    env.execute()
  }

  class MyFlatMap extends FlatMapFunction[String, String] {
    override def flatMap(t: String, collector: Collector[String]): Unit = {
      if (t.equals("white")) {
        collector.collect(t)
      } else if (t.equals("black")) {
        collector.collect(t)
        collector.collect(t)
      }
    }
  }
}