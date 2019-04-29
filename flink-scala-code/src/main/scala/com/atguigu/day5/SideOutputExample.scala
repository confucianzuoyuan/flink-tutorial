package com.atguigu.day5

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputExample {

  val output = new OutputTag[String]("side-output")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val warnings = stream
      .process(new FreezingAlarm)

    warnings.print() // 打印主流
    warnings.getSideOutput(output).print() // 打印测输出流

    env.execute()
  }

  class FreezingAlarm extends ProcessFunction[SensorReading, SensorReading] {
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        ctx.output(output, "传感器ID为：" + value.id + "的传感器温度小于32度！")
      }
      out.collect(value)
    }
  }
}