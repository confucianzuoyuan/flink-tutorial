package com.atguigu.day4

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .process(new FreezingMonitor)

    stream
      .getSideOutput(new OutputTag[String]("freezing-alarms"))
      .print()
    stream.print() // 打印主流
    env.execute()
  }

  // 为什么用`ProcessFunction`? 因为没有keyBy分流
  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
    // 定义侧输出标签
    lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

    // 来一条数据，调用一次
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        // 将报警信息发送到侧输出流
        ctx.output(freezingAlarmOutput, s"传感器ID为 ${value.id} 的传感器发出低于32华氏度的低温报警！")
      }
      out.collect(value) // 在主流上，将数据继续向下发送
    }
  }
}