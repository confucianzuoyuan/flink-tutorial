package com.atguigu.day5

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SensorSwitch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).keyBy(r => r.id)

    val switches = env.fromElements(("sensor_2", 10 * 1000L)).keyBy(r => r._1)

    stream
      .connect(switches)
      .process(new SwitchProcess)
      .print()

    env.execute()
  }

  class SwitchProcess extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {

    lazy val forwardSwitch = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("switch", Types.of[Boolean])
    )

    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (forwardSwitch.value()) {
        out.collect(value)
      }
    }

    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      forwardSwitch.update(true)
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value._2)
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      forwardSwitch.clear()
    }
  }
}