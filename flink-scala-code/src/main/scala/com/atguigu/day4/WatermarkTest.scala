package com.atguigu.day4

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//a 1
//a 2
//a 7
//a 3
//a 3
//a 10
//a 1
//a 1
//a 2
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置流的时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 每隔一分钟插入一次水位线
//    env.getConfig.setAutoWatermarkInterval(60 * 1000L)

    val stream = env.socketTextStream("localhost", 9999, '\n')

    stream
      .map(r => {
        val arr = r.split(" ")
        // 时间戳的单位是毫秒，所以需要ETL一下
        (arr(0), arr(1).toLong * 1000L)
      })
      // 一定要在keyBy之前生成水位线
      // 在插入水位线之前，一定要保证流的并行度是1
      .assignTimestampsAndWatermarks(
        // 水位线策略；默认200ms的机器时间插入一次水位线
        // 水位线 = 当前观察到的事件所携带的最大时间戳 - 最大延迟时间
        WatermarkStrategy
          // 最大延迟时间设置为5s
          .forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            // 告诉系统第二个字段是时间戳，时间戳的单位是毫秒
            override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
          })
      )
      .keyBy(r => r._1)
      // 窗口是左闭右开的区间
      .timeWindow(Time.seconds(5))
      .process(new ProWin)
      .print()

    env.execute()
  }

  class ProWin extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("key为：" + key +" 的窗口为 " + context.window.getStart + "----" + context.window.getEnd + " 中有 " + elements.size + " 个元素")
    }
  }
}