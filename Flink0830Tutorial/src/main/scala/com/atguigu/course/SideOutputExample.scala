package com.atguigu.course

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val readings = env.addSource(new SensorSource).keyBy(_.id)
    val monitoredReadings = readings.process(new FreezingMonitor)

    // 打印侧输出流
    monitoredReadings.getSideOutput(new OutputTag[String]("freezing-alarms")).print()

    // 打印主流
    // readings.print()

    env.execute()
  }

  class FreezingMonitor extends KeyedProcessFunction[String, SensorReading, SensorReading] {
    // 定义一个侧输出标签，侧输出流的类型是 String
    lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

    // 每来到一条元素，都处理一次
    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        // 将报警信息输出到侧输出流去
        ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
      }
      out.collect(value)
    }
  }
}
