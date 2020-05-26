package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempByAggregateAndProcess {
  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      // 第一个参数：增量聚合，第二个参数：全窗口聚合
      .aggregate(new Agg, new WindowResult)
      .print()

    env.execute()
  }

  class WindowResult extends ProcessWindowFunction[(String, Double, Double),
    MinMaxTemp, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      // 迭代器中只有一个值，就是增量聚合函数发送过来的聚合结果
      val minMax = elements.head
      out.collect(MinMaxTemp(key, minMax._2, minMax._3, context.window.getEnd))
    }
  }

  class Agg extends AggregateFunction[SensorReading, (String, Double, Double), (String, Double, Double)] {
    override def createAccumulator(): (String, Double, Double) = {
      ("", Double.MaxValue, Double.MinValue)
    }

    override def add(value: SensorReading, accumulator: (String, Double, Double)): (String, Double, Double) = {
      (value.id, value.temperature.min(accumulator._2), value.temperature.max(accumulator._3))
    }

    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = accumulator

    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
      (a._1, a._2.min(b._2), a._3.max(b._3))
    }
  }
}