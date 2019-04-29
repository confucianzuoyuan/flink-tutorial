package com.atguigu.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RedirectLateEvent1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .process(new MyPro)

    stream.print()
    stream.getSideOutput(new OutputTag[String]("late-readings")).print()

    env.execute()
  }

  class MyPro extends ProcessFunction[(String, Long), String] {

    val lateReadingsOut = new OutputTag[String]("late-readings")

    override def processElement(reading: (String, Long), context: ProcessFunction[(String, Long), String]#Context, collector: Collector[String]): Unit = {
      if (reading._2 < context.timerService().currentWatermark()) {
        context.output(lateReadingsOut, "late reading is comming! ts is " + reading._2)
      } else {
        collector.collect("no late reading is comming")
      }
    }
  }
}