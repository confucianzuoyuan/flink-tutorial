package com.atguigu.day4

import java.lang
import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// nc -lk 9999
// 先使用默认的插入频率，200ms插入一次
// 将`env.getConfig.setAutoWatermarkInterval(60000)`注释掉
//a 1
//a 2
//a 15
//a 1
//a 2
//a 17
//a 12
//a 11
//a 12
//a 13
//a 25
// 添加`env.getConfig.setAutoWatermarkInterval(60000)`
//a 1
//a 2
//a 100
//a 5
//a 8
//a 3
//a 50
//a 51
//a 52
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 应用程序使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 系统每隔一分钟的机器时间插入一次水位线
    env.getConfig.setAutoWatermarkInterval(60000)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        // 第二个元素是时间戳，必须转换成毫秒单位
        (arr(0), arr(1).toLong * 1000)
      })
      // 抽取时间戳和插入水位线
      // 插入水位线的操作一定要紧跟source算子
      .assignTimestampsAndWatermarks(
        // 最大延迟时间设置为5s
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(_._1)
      // 10s的滚动窗口
      .timeWindow(Time.seconds(10))
      .process(new MyProcess)

    stream.print()

    env.execute()
  }

  class MyProcess extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit =
      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + " 的窗口中共有 " + elements.size + " 条数据")
  }
}