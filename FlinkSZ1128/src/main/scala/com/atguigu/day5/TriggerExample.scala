package com.atguigu.day5

import java.sql.Timestamp

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_2"))
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger)
      .process(new WindowResult)

    stream.print()
    env.execute()
  }

  class WindowResult extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("传感器ID为 " + key + " 的传感器窗口中元素的数量是 " + elements.size)
    }
  }

  class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
    // 来一条数据调用一次
    override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      // 如果firstSeen为false，也就是当碰到第一条元素的时候
      if (!firstSeen.value()) {
        // 假设第一条事件来的时候，机器时间是1234ms，t是多少？t是2000ms
        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))
        ctx.registerProcessingTimeTimer(t) // 在2000ms注册一个定时器
        ctx.registerProcessingTimeTimer(window.getEnd) // 在窗口结束时间注册一个定时器
        firstSeen.update(true)
      }

      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    // 注册的定时器的回调函数
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println("回调函数触发时间：" + new Timestamp(time))
      if (time == window.getEnd) {
        TriggerResult.FIRE_AND_PURGE
      } else {
        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))
        if (t < window.getEnd) {
          ctx.registerProcessingTimeTimer(t)
        }
        TriggerResult.FIRE
      }
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      // SingleTon, 单例模式，只会被初始化一次
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }
}