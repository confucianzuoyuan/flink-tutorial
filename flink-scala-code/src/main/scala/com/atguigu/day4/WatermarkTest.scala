package com.atguigu.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
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

    val stream = env.socketTextStream("localhost", 9999, '\n')

    stream
      .map(r => {
        val arr = r.split(" ")
        // 时间戳的单位是毫秒，所以需要ETL一下
        (arr(0), arr(1).toLong * 1000L)
      })
      // 提取时间戳，设置水位线
      .assignTimestampsAndWatermarks(
        // 最大延迟时间设置了5s
        // 水位线 = 系统观察到的事件所携带的最大时间戳 - 最大延迟时间a
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(r => r._1)
      .timeWindow(Time.seconds(5))
      .process(new ProWin)
      .print()

    env.execute()
  }

  class ProWin extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口为 " + context.window.getStart + "----" + context.window.getEnd + " 中有 " + elements.size + " 个元素")
    }
  }
}