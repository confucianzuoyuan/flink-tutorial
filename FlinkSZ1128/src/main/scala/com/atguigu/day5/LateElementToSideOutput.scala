package com.atguigu.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object LateElementToSideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val readings = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
//      .assignTimestampsAndWatermarks(
//        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.milliseconds(1)) {
//          override def extractTimestamp(element: (String, Long)): Long = element._2
//        }
//      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .sideOutputLateData(
        new OutputTag[(String, Long)]("late")
      )
      .process(new CountFunction)


    readings.print()
    readings.getSideOutput(new OutputTag[(String, Long)]("late")).print()

    env.execute()
  }

  class CountFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(context.window.getStart + "到" + context.window.getEnd + "的窗口闭合了！")
    }
  }
}