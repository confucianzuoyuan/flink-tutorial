package com.atguigu.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object WatermarkBroadcast {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1 = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)

    //a 1
    //a 2
    //a 3
    val stream2 = env
      .socketTextStream("localhost", 9998, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)

    //a 100
    //a 101
    //a 102
    //a 103
    stream1
      .connect(stream2)
      .process(new MyCoProcess)
      .print()

    env.execute()
  }

  class MyCoProcess extends CoProcessFunction[(String, Long), (String, Long), String] {
    // 当来自第一条流的数据到达时，调用
    override def processElement1(value: (String, Long), ctx: CoProcessFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect("来自第一条流，水位线是：" + ctx.timerService().currentWatermark())
    }

    // 当来自第二条流的数据到达时，调用
    override def processElement2(value: (String, Long), ctx: CoProcessFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect("来自第二条流，水位线是：" + ctx.timerService().currentWatermark())
    }
  }
}