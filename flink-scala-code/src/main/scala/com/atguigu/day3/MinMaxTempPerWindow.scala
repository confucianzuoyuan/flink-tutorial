package com.atguigu.day3

import java.sql.Timestamp

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).filter(r => r.id.equals("sensor_1"))

    stream
      .keyBy(r => r.id)
      .timeWindow(Time.seconds(5))
      .process(new MinMaxProcessFunction)
      .print()

    env.execute()
  }

  case class MinMaxTemp(id: String, minTemp: Double, maxTemp: Double, window: String)

  class MinMaxProcessFunction extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {
      val temps = elements.map(_.temperature)
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd

      out.collect(MinMaxTemp(key, temps.min, temps.max, "窗口：" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd)))
    }
  }
}