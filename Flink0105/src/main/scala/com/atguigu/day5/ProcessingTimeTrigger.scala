package com.atguigu.day5

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessingTimeTrigger {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_1"))
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger)
      .process(new WindowCount)

    stream.print()

    env.execute()
  }

  // 只在整数秒和窗口结束时间时触发窗口计算！
  class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
    // 每来一条数据都要调用一次！
    override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      // 默认值为false
      // 当第一条事件来的时候，会在后面的代码中将firstSeen置为true
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      if (!firstSeen.value()) {
        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))
        ctx.registerProcessingTimeTimer(t) // 在第一条数据的时间戳之后的整数秒注册一个定时器
        ctx.registerProcessingTimeTimer(window.getEnd) // 在窗口结束事件注册一个定时器
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      // 在onElement函数中，我们注册过窗口结束时间的定时器
      // 将窗口闭合的默认触发操作override掉了。
      if (time == window.getEnd) {
        // 在窗口闭合时，触发计算并清空窗口
        TriggerResult.FIRE_AND_PURGE
      } else {
        // 1233ms后面的整数秒是2000ms
        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))
        // 保证t小于窗口结束时间
        if (t < window.getEnd) {
          ctx.registerProcessingTimeTimer(t)
        }
        println("定时器触发的时间为：" + time)
        TriggerResult.FIRE
      }
    }

    // 定时器函数，在水位线到达time时，触发
    override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {
      // 状态变量是一个单例！
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      firstSeen.clear()
    }
  }

  class WindowCount extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("窗口中有 " + elements.size + " 条数据！窗口结束时间是" + context.window.getEnd)
    }
  }
}