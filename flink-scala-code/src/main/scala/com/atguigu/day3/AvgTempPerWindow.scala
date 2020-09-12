package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AvgTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(r => r.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTemp)
      .print()

    env.execute()
  }

  class AvgTemp extends AggregateFunction[SensorReading, (String, Double, Long), (String, Double)] {
    override def createAccumulator(): (String, Double, Long) = ("", 0.0, 0L)

    override def add(value: SensorReading, accumulator: (String, Double, Long)): (String, Double, Long) = {
      (value.id, accumulator._2 + value.temperature, accumulator._3 + 1)
    }

    override def getResult(accumulator: (String, Double, Long)): (String, Double) = {
      (accumulator._1, accumulator._2 / accumulator._3)
    }

    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }
}