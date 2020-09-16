package com.atguigu.day6

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object GenPeriodicWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(10 * 1000L)

    val stream = env.socketTextStream("localhost", 9999, '\n')

    stream
      .map(line => {
      val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignTimestampsAndWatermarks(
        // generate periodic watermarks
        new AssignerWithPeriodicWatermarks[(String, Long)] {
          val bound = 10 * 1000L // 最大延迟时间
          var maxTs = Long.MinValue + bound + 1 // 当前观察到的最大时间戳

          // 用来生成水位线
          // 默认200ms调用一次
          override def getCurrentWatermark: Watermark = {
            println("generate watermark!!!" + (maxTs - bound - 1) + "ms")
            new Watermark(maxTs - bound - 1)
          }

          // 每来一条数据都会调用一次
          override def extractTimestamp(t: (String, Long), l: Long): Long = {
            println("extract timestamp!!!")
            maxTs = maxTs.max(t._2) // 更新观察到的最大事件时间
            t._2 // 抽取时间戳
          }
        }
      )
      .keyBy(r => r._1)
      .process(new KeyedProcessFunction[String, (String, Long), String] {
        override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), String]#Context, collector: Collector[String]): Unit = {
          collector.collect("current watermark is " + context.timerService().currentWatermark())
        }
      })
      .print()

    env.execute()
  }
}