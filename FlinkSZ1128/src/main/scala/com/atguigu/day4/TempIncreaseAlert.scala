package com.atguigu.day4

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 如果某一个传感器连续1s中温度上升，报警！
object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    stream.print()
    env.execute()
  }

  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
    // 用来存储最近一次的温度
    // 当保存检查点的时候，会将状态变量保存到状态后端
    // 默认状态后端是内存，也可以配置hdfs等为状态后端
    // 懒加载，当运行到process方法的时候，才会惰性赋值
    // 状态变量只会被初始化一次
    // 根据`last-temp`这个名字到状态后端去查找，如果状态后端中没有，那么初始化
    // 如果在状态后端中存在`last-temp`的状态变量，直接懒加载
    // 默认值是`0.0`
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double](
        "last-temp",
        Types.of[Double]
      )
    )

    // 存储定时器时间戳的状态变量
    // 默认值是`0L`
    lazy val currentTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long](
        "timer",
        Types.of[Long]
      )
    )

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      // 获取最近一次的温度, 使用`.value()`
      val prevTemp = lastTemp.value()
      // 将当前温度存入状态变量, `.update()`
      lastTemp.update(value.temperature)

      // 获取定时器状态变量中的时间戳
      val curTimerTimestamp = currentTimer.value()

      // 温度：1，2，3，4，5，2
      if (prevTemp == 0.0 || value.temperature < prevTemp) {
        // 如果当前温度是第一个温度读数，或者温度下降
        // 删除状态变量保存的时间戳对应的定时器
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        currentTimer.clear() // 清空状态变量
      } else if (value.temperature > prevTemp && curTimerTimestamp == 0L) {
        // 如果温度上升，且保存定时器时间戳的状态变量为空，就注册一个定时器
        // 注册一个1s之后的定时器
        val timerTs = ctx.timerService().currentProcessingTime() + 1000L
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        // 将时间戳存入状态变量
        currentTimer.update(timerTs)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器ID为 " + ctx.getCurrentKey + " 的传感器，温度连续1秒钟上升了！")
      currentTimer.clear() // 清空状态变量
    }
  }
}