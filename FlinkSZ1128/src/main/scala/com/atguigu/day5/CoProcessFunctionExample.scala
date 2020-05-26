package com.atguigu.day5

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 无限流
    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)

    // 有限流
    val filterSwitches = env
      .fromElements(
        ("sensor_2", 10 * 1000L)
//        ("sensor_7", 60 * 1000L)
      )
      .keyBy(_._1)

    readings
      .connect(filterSwitches)
      .process(new ReadingFilter)
      .print()

    env.execute()
  }

  class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
    // 初始化传送数据的开关，默认值是false
    // 只针对当前key可见的状态变量
    lazy val forwardingEnabled = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("filter-switch", Types.of[Boolean])
    )

    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 处理第一条流，无限流
      // 如果开关是true，将传感器数据向下游发送
      if (forwardingEnabled.value()) {
        out.collect(value)
      }
    }

    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 处理第二条流，有限流，只会被调用两次

      forwardingEnabled.update(true) // 打开开关

      // `value._2`是开关打开的时间
      val timerTs = ctx.timerService().currentProcessingTime() + value._2

      ctx.timerService().registerProcessingTimeTimer(timerTs)
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      forwardingEnabled.update(false) // 关闭开关
    }
  }
}