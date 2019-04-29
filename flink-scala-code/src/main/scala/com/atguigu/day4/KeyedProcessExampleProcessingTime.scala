package com.atguigu.day4

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProcessExampleProcessingTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val stream = env.socketTextStream("localhost", 9999, '\n')

    stream
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1))
      })
      .keyBy(r => r._1)
      .process(new MyKeyed)
      .print()

    env.execute()
  }

  class MyKeyed extends KeyedProcessFunction[String, (String, String), String] {
    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, out: Collector[String]): Unit = {
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("定时器触发了！触发时间是：" + new Timestamp(timestamp))
    }
  }
}