package com.atguigu.course

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PeriodicWatermarkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .addSource(new SensorSource)
      // 如果提前知道数据的时间戳是单调递增，也就是说没有乱序存在，可以用以下方法抽取时间戳
      // .assignAscendingTimestamps(r => r.timestamp)
      .assignTimestampsAndWatermarks(
        new PeriodicAssigner
      )
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new MyWindow)

    stream.print()

    env.execute()
  }

  class MyWindow extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("在传感器 " + key + " 中，窗口内共有： " + elements.size.toString + " 个读数")
    }
  }

  class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
    // 最大延迟时间是 1 秒钟
    val bound : Long = 1000
    // 如果最大延迟时间是 0，说明没有乱序存在
    // val bound : Long = 0L
    // 用来保存观察到的最大事件时间戳
    var maxTs : Long = Long.MinValue

    // 每来到一个元素，都会调用一次
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
      maxTs = maxTs.max(element.timestamp)
      element.timestamp
    }

    // 这个函数什么时候调用呢？ 系统插入水位线的时候，默认 200ms 插入一次
    override def getCurrentWatermark: Watermark = {
      // 水位线计算公式
      new Watermark(maxTs - bound)
    }
  }
}
