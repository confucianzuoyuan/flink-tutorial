package com.atguigu.day5

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object RedirectLateEvent {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 9999, '\n')

    val readings = stream
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
//      .assignAscendingTimestamps(_._2)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps[(String, Long)]()
          .withTimestampAssigner(
            new SerializableTimestampAssigner[(String, Long)] {
              override def extractTimestamp(t: (String, Long), l: Long): Long = t._2
            }
          )
      )
      .keyBy(r => r._1)
      .timeWindow(Time.seconds(5))
      .sideOutputLateData(new OutputTag[(String, Long)]("late-readings"))
      .process(new CountWindow)

    readings.print()
    readings.getSideOutput(new OutputTag[(String, Long)]("late-readings")).print()

    env.execute()
  }

  class CountWindow extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("一共有" + elements.size + " 个元素")
    }
  }
}