package com.atguigu.course

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object LateEventToSideOutputExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.socketTextStream("localhost", 9999, '\n')
    val mainStream = stream
      .map(r => {
        val arr = r.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(r => r._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sideOutputLateData(new OutputTag[(String, Long)]("late-readings"))
      .process(new MyWindow)

    mainStream.print()
    mainStream.getSideOutput(new OutputTag[(String, Long)]("late-readings")).print()

    env.execute()
  }

  class MyWindow extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("来自主流： " + elements.size.toString)
    }
  }
}
