package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AvgTempByAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTempAgg)
      .print()

    env.execute()
  }

  //(一个key，一个窗口)对应一个累加器
  // 第一个泛型：流中元素的类型
  // 第二个泛型：累加器的类型，元组(传感器ID，来了多少条温度读数，来的温度读数的总和是多少)
  // 第三个泛型：增量聚合函数的输出类型，元组(传感器ID，窗口温度平均值)
  class AvgTempAgg extends AggregateFunction[SensorReading, (String, Long, Double), (String, Double)] {
    // 创建空累加器
    override def createAccumulator(): (String, Long, Double) = ("", 0L, 0.0)

    // 聚合逻辑是什么
    override def add(value: SensorReading, accumulator: (String, Long, Double)): (String, Long, Double) = {
      (value.id, accumulator._2 + 1, accumulator._3 + value.temperature)
    }

    // 窗口闭合时，输出的结果是什么？
    override def getResult(accumulator: (String, Long, Double)): (String, Double) = {
      (accumulator._1, accumulator._3 / accumulator._2)
    }

    // 两个累加器合并的逻辑是什么？
    override def merge(a: (String, Long, Double), b: (String, Long, Double)): (String, Long, Double) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }
}