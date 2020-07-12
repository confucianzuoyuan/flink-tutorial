package com.atguigu.day4

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

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

    // 初始化一个状态变量，用来保存最近一次的温度值
    // 懒加载，惰性赋值
    // 当执行到process算子时，才会初始化，所以是懒加载
    // 为什么不直接使用scala的变量呢？比如：var lastTemp: Double = _
    // 通过配置，状态变量可以通过检查点操作，保存在hdfs里面
    // 当程序故障时，可以从最近一次检查点恢复
    // 所以要有一个名字`last-temp`和变量的类型（需要明确告诉flink状态变量的类型）
    // 状态变量只会被初始化一次，运行程序时，如果没有这个状态变量，就初始化一个
    // 如果有这个状态变量，直接读取
    // 所以是`单例模式`
    // 默认值是0.0
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
    )

    // 用来保存报警定时器的时间戳，默认值是0L
    lazy val timerTs = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts", Types.of[Long])
    )

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      // 获取最近一次温度, 需要使用`.value()`方法
      // 如果来的是第一条温度，那么prevTemp是0.0
      val prevTemp = lastTemp.value()
      // 将来的温度值更新到lastTemp状态变量, 使用update方法
      lastTemp.update(value.temperature)

      val curTimerTs = timerTs.value()

      if (prevTemp == 0.0 || value.temperature < prevTemp) {
        // 如果来的温度是第一条温度，或者来的温度小于最近一次温度
        // 删除报警定时器
        ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
        // 清空保存定时器时间戳的状态变量, 使用clear方法
        timerTs.clear()
      } else if (value.temperature > prevTemp && curTimerTs == 0L) {
        // 来的温度大于最近一次温度，并且我们没有注册报警定时器，因为curTimerTs等于0L
        val ts = ctx.timerService().currentProcessingTime() + 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        timerTs.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器ID为：" + ctx.getCurrentKey + " 的传感器温度连续1s上升！")
      timerTs.clear() // 别忘了清空定时器
    }
  }
}