package com.atguigu.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// nc -lk 9999
//a 1
//a 12
//a 23
object EventTimeOnTimer {
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
      // 插入水位线的时候，时间戳 - 1ms
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new MyKeyedProcess)

    stream.print()
    env.execute()
  }

  class MyKeyedProcess extends KeyedProcessFunction[String, (String, Long), String] {
    // 来一条数据调用一次
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      // 在当前元素时间戳的10s钟以后，注册一个定时器，定时器的业务逻辑由`onTimer`函数实现
      ctx.timerService().registerEventTimeTimer(value._2 + 10 * 1000L)
      out.collect("当前水位线是：" + ctx.timerService().currentWatermark())
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("位于时间戳：" + timestamp + "的定时器触发了！")
    }
  }
}