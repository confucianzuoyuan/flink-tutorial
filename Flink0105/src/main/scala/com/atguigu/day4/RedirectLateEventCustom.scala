package com.atguigu.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//a 1
//a 2
//a 3
//a 1
object RedirectLateEventCustom {
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
      .process(new LateEventProc)

    stream.print()
    stream
      .getSideOutput(new OutputTag[String]("late"))
      .print()

    env.execute()
  }

  class LateEventProc extends ProcessFunction[(String, Long), (String, Long)] {

    val late = new OutputTag[String]("late")

    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      // 如果到来的元素所包含的时间戳小于当前数据流的水位线，即为迟到元素
      if (value._2 < ctx.timerService().currentWatermark()) {
        // 将迟到元素发送到侧输出流中去
        ctx.output(late, "迟到事件来了！")
      } else {
        out.collect(value)
      }
    }
  }
}