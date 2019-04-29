package com.atguigu.day3

import java.sql.Timestamp

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempWithAggAndWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).filter(r => r.id.equals("sensor_1"))

    stream
      .keyBy(r => r.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new Agg, new Win)
      .print()

    env.execute()
  }

  class Agg extends AggregateFunction[SensorReading, (Double, Double), (Double, Double)] {
    override def createAccumulator(): (Double, Double) = (Double.MaxValue, Double.MinValue)

    override def add(value: SensorReading, accumulator: (Double, Double)): (Double, Double) = {
      (accumulator._1.min(value.temperature), accumulator._2.max(value.temperature))
    }

    override def getResult(accumulator: (Double, Double)): (Double, Double) = accumulator

    override def merge(a: (Double, Double), b: (Double, Double)): (Double, Double) = ???
  }

  class Win extends ProcessWindowFunction[(Double, Double), MinMaxTemp, String, TimeWindow] {
    // 迭代器中只有一个值，就是窗口闭合时，增量聚合函数发送过来的聚合结果
    override def process(key: String, context: Context, elements: Iterable[(Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val e = elements.head
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd
      out.collect(MinMaxTemp(key, e._1, e._2, "窗口：" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd)))
    }
  }

  case class MinMaxTemp(id: String, minTemp: Double, maxTemp: Double, window: String)
}