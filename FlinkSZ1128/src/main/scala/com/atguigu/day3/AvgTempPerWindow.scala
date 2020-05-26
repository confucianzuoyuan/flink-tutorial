package com.atguigu.day3

import com.atguigu.day2.SensorSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AvgTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    stream
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTempFunction)
      .print()

    env.execute()
  }

  // 平均温度值 = 总的温度值 / 温度的条数
  class AvgTempFunction extends AggregateFunction[(String, Double), (String, Double, Long), (String, Double)] {
    // 创建累加器
    override def createAccumulator(): (String, Double, Long) = ("", 0.0, 0L)

    // 每来一条数据，如何累加？
    override def add(value: (String, Double), accumulator: (String, Double, Long)): (String, Double, Long) = {
      (value._1, accumulator._2 + value._2, accumulator._3 + 1)
    }

    override def getResult(accumulator: (String, Double, Long)): (String, Double) = {
      (accumulator._1, accumulator._2 / accumulator._3)
    }

    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }
}