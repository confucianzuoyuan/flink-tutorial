package com.atguigu.day3

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// nc -lk 9999
// a 1
// a 8
// a 1
// a 1
// a 1
// a 15
// a 1
// a 1
object EventTimeExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 系统默认每隔200ms插入一次水位线

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        // 事件时间的单位必须是毫秒！
        (arr(0), arr(1).toLong * 1000L)
      })
      // 分配时间戳和水位线一定要在keyBy之前进行！
      // 水位线 = 系统观察到的元素携带的最大时间戳 - 最大延迟时间
      .assignTimestampsAndWatermarks(
        // 设置事件的最大延迟时间是5s
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          // 告诉系统，时间戳是元组的第二个字段
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new WindowResult)

    stream.print()
    env.execute()
  }

  class WindowResult extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart) + " ~ " + new Timestamp(context.window.getEnd) + " 的窗口中有 " + elements.size + " 个元素！")
    }
  }
}