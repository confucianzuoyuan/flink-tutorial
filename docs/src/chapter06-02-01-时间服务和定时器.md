### 时间服务和定时器

Context和OnTimerContext所持有的TimerService对象拥有以下方法:

* `currentProcessingTime(): Long` 返回当前处理时间
* `currentWatermark(): Long` 返回当前水位线的时间戳
* `registerProcessingTimeTimer(timestamp: Long): Unit` 会注册当前key的processing time的timer。当processing time到达定时时间时，触发timer。
* `registerEventTimeTimer(timestamp: Long): Unit` 会注册当前key的event time timer。当水位线大于等于定时器注册的时间时，触发定时器执行回调函数。
* `deleteProcessingTimeTimer(timestamp: Long): Unit` 删除之前注册处理时间定时器。如果没有这个时间戳的定时器，则不执行。
* `deleteEventTimeTimer(timestamp: Long): Unit` 删除之前注册的事件时间定时器，如果没有此时间戳的定时器，则不执行。

当定时器timer触发时，执行回调函数onTimer()。processElement()方法和onTimer()方法是同步（不是异步）方法，这样可以避免并发访问和操作状态。

针对每一个key和timestamp，只能注册一个定期器。也就是说，每一个key可以注册多个定时器，但在每一个时间戳只能注册一个定时器。KeyedProcessFunction默认将所有定时器的时间戳放在一个优先队列中。在Flink做检查点操作时，定时器也会被保存到状态后端中。

举个例子说明KeyedProcessFunction如何操作KeyedStream。

下面的程序展示了如何监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连续上升，报警。

```scala
val warnings = readings
  // key by sensor id
  .keyBy(_.id)
  // apply ProcessFunction to monitor temperatures
  .process(new TempIncreaseAlertFunction)
```

看一下TempIncreaseAlertFunction如何实现, 程序中使用了ValueState这样一个状态变量, 后面会详细讲解。

```scala
class TempIncreaseAlertFunction
  extends KeyedProcessFunction[String, SensorReading, String] {
  // 保存上一个传感器温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", Types.of[Double])
  )

  // 保存注册的定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer", Types.of[Long])
  )

  override def processElement(r: SensorReading,
                              ctx: KeyedProcessFunction[String,
                                SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    // get previous temperature
    // 取出上一次的温度
    val prevTemp = lastTemp.value()
    // update last temperature
    // 将当前温度更新到上一次的温度这个变量中
    lastTemp.update(r.temperature)

    val curTimerTimestamp = currentTimer.value()
    if (prevTemp == 0.0 || r.temperature < prevTemp) {
      // temperature decreased; delete current timer
      // 温度下降或者是第一个温度值，删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      // 清空状态变量
      currentTimer.clear()
    } else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
      // temperature increased and we have not set a timer yet
      // set processing time timer for now + 1 second
      // 温度上升且我们并没有设置定时器
      val timerTs = ctx.timerService().currentProcessingTime() + 1000
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      // remember current timer
      currentTimer.update(timerTs)
    }
  }

  override def onTimer(ts: Long,
                       ctx: KeyedProcessFunction[String,
                        SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect("传感器id为: "
      + ctx.getCurrentKey
      + "的传感器温度值已经连续1s上升了。")
    currentTimer.clear()
  }
}
```

