package com.atguigu.course

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 教程 6-12
// 需求：计算窗口中最大温度和最小温度，附加上窗口结束时间
// 全窗口聚合函数需要将窗口中的所有元素都缓存下来，如果元素很多，那么存储压力就很大
object ProcessWindowFunctionExample {

  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorData = env.addSource(new SensorSource)

    val minMaxTempPerWindow = sensorData
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempProcessFunction)

    minMaxTempPerWindow.print()

    env.execute()
  }

  class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
    // 当窗口闭合的时候调用，Iterable里面包含了窗口收集的所有元素
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {
      val temps = elements.map(_.temperature)
      val windowEnd = context.window.getEnd

      out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
    }
  }
}
