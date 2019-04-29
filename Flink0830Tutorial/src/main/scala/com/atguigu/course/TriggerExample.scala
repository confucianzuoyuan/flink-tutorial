package com.atguigu.course

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.addSource(new SensorSource)

    stream
      .assignAscendingTimestamps(r => r.timestamp)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .trigger(new OneSecondIntervalTrigger)
      .process(new TriggerWindowFunction)
      .print()

    env.execute()
  }

  class TriggerWindowFunction extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("触发器触发，当前窗口元素数量是： " + elements.size.toString + " 传感器id是： " + key)
    }
  }

  class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
    override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val firstSeen : ValueState[Boolean] = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen", Types.of[Boolean])
      )

      if (!firstSeen.value()) {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        ctx.registerEventTimeTimer(t)
        ctx.registerEventTimeTimer(window.getEnd)
        firstSeen.update(true)
      }

      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println(time)
      if (time == window.getEnd) {
        TriggerResult.FIRE_AND_PURGE
      } else {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        if (t < window.getEnd) {
          ctx.registerEventTimeTimer(t)
        }
        TriggerResult.FIRE
      }
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      val firstSeen : ValueState[Boolean] = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }
}
