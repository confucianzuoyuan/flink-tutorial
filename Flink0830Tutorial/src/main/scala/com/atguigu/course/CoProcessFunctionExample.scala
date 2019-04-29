package com.atguigu.course

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 传感器读数
    val readings = env.addSource(new SensorSource)

    // 开关流
    val filterSwitches = env.fromCollection(Seq(
      ("sensor_2", 10 * 1000L) // id 为 2 的传感器读数放行 10s
    ))

    val forwardedReadings = readings.keyBy(_.id).connect(filterSwitches.keyBy(_._1)).process(new ReadingFilter)

    forwardedReadings.print()

    env.execute()
  }

  class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
    // 传送数据的开关
    // getRuntimeContext.getState首先去查找当前 key 所对应的状态中有没有名字叫做 filterSwitch 的状态变量
    // 没有的话，就初始化一个
    lazy val forwardingEnabled : ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean])
    )

    // 保存关闭开关的定时器的时间戳
    lazy val disableTimer : ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement1(value: SensorReading,
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      // 如果值为 true ，也就是说开关被打开了
      if (forwardingEnabled.value()) {
        out.collect(value)
      }
    }

    // 来自第二条流的事件只有一条，所以 processElement2 只会被调用一次
    override def processElement2(value: (String, Long),
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      // 当碰到来自第二条流的事件，就可以把开关置为 true，本程序打开的是 sensor_2 传感器
      forwardingEnabled.update(true)

      // 获取 10s 之后的机器时间戳
      val timerTimestamp = ctx.timerService().currentProcessingTime() + value._2

      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
      disableTimer.update(timerTimestamp)
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
      // 将开关关闭掉
      forwardingEnabled.clear()
      disableTimer.clear()
    }
  }
}
