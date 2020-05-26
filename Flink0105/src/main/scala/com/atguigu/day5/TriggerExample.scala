package com.atguigu.day5

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger)
      .process(new WindowCount)

    stream.print()

    env.execute()
  }

  // 只在整数秒和窗口结束时间时触发窗口计算！
  class OneSecondIntervalTrigger extends Trigger[(String, Long), TimeWindow] {
    // 每来一条数据都要调用一次！
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      // 默认值为false
      // 当第一条事件来的时候，会在后面的代码中将firstSeen置为true
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      // 当第一条数据来的时候，!firstSeen.value()为true
      // 仅对第一条数据注册定时器
      // 这里的定时器指的是：onEventTime函数！
      if (!firstSeen.value()) {
        // 第一条数据`a 1234`来的时候，水位线是：-9223372036854775808
        // 过了200ms，插入了一个水位线1234 - 1 = 1233
        // 水位线后面的整数秒是：-9223372036854774000
        println("第一条数据来了！当前水位线是：" + ctx.getCurrentWatermark)
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        println("第一条数据来了以后，注册的定时器的整数秒的时间戳是：" + t)
        ctx.registerEventTimeTimer(t) // 在第一条数据的时间戳之后的整数秒注册一个定时器
        ctx.registerEventTimeTimer(window.getEnd) // 在窗口结束事件注册一个定时器
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    // 定时器函数，在水位线到达time时，触发
    override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      // 在onElement函数中，我们注册过窗口结束时间的定时器
      if (time == window.getEnd) {
        // 在窗口闭合时，触发计算并清空窗口
        TriggerResult.FIRE_AND_PURGE
      } else {
        // 1233ms后面的整数秒是2000ms
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        // 保证t小于窗口结束时间
        if (t < window.getEnd) {
          println("注册的定时器的整数秒的时间戳是：" + t)
          // 这里注册的定时器还是onEventTime函数
          ctx.registerEventTimeTimer(t)
        }
        // 触发窗口计算
        println("在 " + time + " 触发了窗口计算！")
        TriggerResult.FIRE
      }
    }

    override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {
      // 状态变量是一个单例！
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      firstSeen.clear()
    }
  }

  class WindowCount extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口中有 " + elements.size + " 条数据！窗口结束时间是" + context.window.getEnd)
    }
  }
}