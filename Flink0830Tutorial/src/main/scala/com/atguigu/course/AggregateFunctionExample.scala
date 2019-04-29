package com.atguigu.course

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 需求：计算窗口的温度平均值
// 使用增量聚合的方式计算平均值
// 教程 6-11
object AggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorData = env.addSource(new SensorSource)
    val avgTempPerWindow = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      // 注意下面是 aggregate
      .aggregate(new AvgTempFunction)

    avgTempPerWindow.print()

    env.execute()
  }

  class AvgTempFunction extends AggregateFunction[(String, Double), (String, Double, Int), (String, Double)] {
    override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

    override def add(value: (String, Double), accumulator: (String, Double, Int)): (String, Double, Int) = {
      // 第一个元素将id保留下来，元组的第二个元素是传感器读数的总数，第三个元素是一共有多少个传感器读数
      (value._1, value._2 + accumulator._2, accumulator._3 + 1)
    }

    override def getResult(accumulator: (String, Double, Int)): (String, Double) = {
      // 第一个元素将 id 保留了下来，第二个元素：平均值 = 温度总和 / 一共有多少个温度读数
      (accumulator._1, accumulator._2 / accumulator._3)
    }

    override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }
}
