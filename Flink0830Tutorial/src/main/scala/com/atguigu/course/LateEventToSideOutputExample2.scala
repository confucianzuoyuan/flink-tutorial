package com.atguigu.course

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object LateEventToSideOutputExample2 {
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
      // 最大延迟时间是 0
      .assignAscendingTimestamps(r => r._2)
      .keyBy(_._1)
      .process(new MyKeyedProcessFunction)

    mainStream.getSideOutput(new OutputTag[(String, Long)]("late-readings")).print()

    env.execute()
  }

  class MyKeyedProcessFunction extends KeyedProcessFunction[String, (String, Long), (String, Long)] {
    val lateReadingOut = new OutputTag[(String, Long)]("late-readings")

    override def processElement(value: (String, Long),
                                ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context,
                                out: Collector[(String, Long)]): Unit = {
      // 来的元素的事件时间小于当前系统的水位线
      if (value._2 < ctx.timerService().currentWatermark()) {
        // 迟到的元素，发送到侧输出流中去
        ctx.output(lateReadingOut, value)
      } else {
        // 没有迟到的元素，正常输出
        out.collect(value)
      }
    }
  }

}
