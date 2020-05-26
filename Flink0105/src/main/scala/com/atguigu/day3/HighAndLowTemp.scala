package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighAndLowTemp {

  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new HighAndLowAgg, new WindowResult)
      .print()

    env.execute()
  }

  class HighAndLowAgg extends AggregateFunction[SensorReading, (String, Double, Double), (String, Double, Double)] {
    // 最小温度值的初始值是Double的最大值
    // 最大温度值的初始值是Double的最小值
    override def createAccumulator(): (String, Double, Double) = ("", Double.MaxValue, Double.MinValue)

    override def add(value: SensorReading, accumulator: (String, Double, Double)): (String, Double, Double) = {
      (value.id, value.temperature.min(accumulator._2), value.temperature.max(accumulator._3))
    }

    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = accumulator

    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
      (a._1, a._2.min(b._2), a._3.max(b._3))
    }
  }

  class WindowResult extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val minMax = elements.head
      out.collect(MinMaxTemp(key, minMax._2, minMax._3, context.window.getEnd))
    }
  }
}