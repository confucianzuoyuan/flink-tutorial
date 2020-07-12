package com.atguigu.day4

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

    // 第一条流，是一条无限流
    val readings = env.addSource(new SensorSource)

    // 第二条流，是一条有限流，只有一个元素
    // 用来做开关，对`sensor_2`的数据放行10s
    val switches = env.fromElements(
      ("sensor_2", 10 * 1000L),
      ("sensor_10", 5 * 1000L)
    )

    val result = readings
      .connect(switches)
      // 将相同key的数据放在一起处理
      .keyBy(_.id, _._1) // on readings.id = switches._1
      .process(new ReadingFilter)

    result.print()

    env.execute()
  }

  class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
    // 初始值是false
    // 每一个key都有对应的状态变量
    lazy val forwardingEnabled = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("switch", Types.of[Boolean])
    )

    // 处理来自传感器的流数据
    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 如果开关是true，就允许数据流向下发送
      if (forwardingEnabled.value()) {
        out.collect(value)
      }
    }

    // 处理来自开关流的数据
    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 打开开关
      forwardingEnabled.update(true)
      // 开关元组的第二个值就是放行时间
      val ts = ctx.timerService().currentProcessingTime() + value._2
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      // 关闭开关
      forwardingEnabled.clear()
    }
  }
}