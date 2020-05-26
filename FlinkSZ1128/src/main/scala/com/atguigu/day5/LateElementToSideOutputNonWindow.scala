package com.atguigu.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object LateElementToSideOutputNonWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val readings = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .process(new LateToSideOutput)

    readings.print()
    readings.getSideOutput(new OutputTag[String]("late")).print()

    env.execute()
  }

  class LateToSideOutput extends ProcessFunction[(String, Long), String] {
    val lateReadingOutput = new OutputTag[String]("late")

    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), String]#Context, out: Collector[String]): Unit = {
      if (value._2 < ctx.timerService().currentWatermark()) {
        ctx.output(lateReadingOutput, "迟到事件来了！")
      } else {
        out.collect("没有迟到的事件来了！")
      }
    }
  }
}