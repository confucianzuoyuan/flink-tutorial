### CoProcessFunction

对于两条输入流，DataStream API提供了CoProcessFunction这样的low-level操作。CoProcessFunction提供了操作每一个输入流的方法: processElement1()和processElement2()。类似于ProcessFunction，这两种方法都通过Context对象来调用。这个Context对象可以访问事件数据，定时器时间戳，TimerService，以及side outputs。CoProcessFunction也提供了onTimer()回调函数。下面的例子展示了如何使用CoProcessFunction来合并两条流。

```scala
// ingest sensor stream
val readings: DataStream[SensorReading] = ...

// filter switches enable forwarding of readings
val filterSwitches: DataStream[(String, Long)] = env
  .fromCollection(Seq(
    ("sensor_2", 10 * 1000L),
    ("sensor_7", 60 * 1000L)
  ))

val forwardedReadings = readings
  // connect readings and switches
  .connect(filterSwitches)
  // key by sensor ids
  .keyBy(_.id, _._1)
  // apply filtering CoProcessFunction
  .process(new ReadingFilter)
```

```scala
class ReadingFilter
  extends CoProcessFunction[SensorReading,
    (String, Long), SensorReading] {
  // switch to enable forwarding
  // 传送数据的开关
  lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext
    .getState(
      new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean])
    )

  // hold timestamp of currently active disable timer
  lazy val disableTimer: ValueState[Long] = getRuntimeContext
    .getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

  override def processElement1(reading: SensorReading,
                               ctx: CoProcessFunction[SensorReading,
                                (String, Long), SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {
    // check if we may forward the reading
    // 决定我们是否要将数据继续传下去
    if (forwardingEnabled.value()) {
      out.collect(reading)
    }
  }

  override def processElement2(switch: (String, Long),
                               ctx: CoProcessFunction[SensorReading,
                                (String, Long), SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {
    // enable reading forwarding
    // 允许继续传输数据
    forwardingEnabled.update(true)
    // set disable forward timer
    val timerTimestamp = ctx.timerService().currentProcessingTime()
     + switch._2
  
    val curTimerTimestamp = disableTimer.value()

    if (timerTimestamp > curTimerTimestamp) {
      // remove current timer and register new timer
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
      disableTimer.update(timerTimestamp)
    }
  }

  override def onTimer(ts: Long,
                       ctx: CoProcessFunction[SensorReading,
                        (String, Long), SensorReading]#OnTimerContext,
                       out: Collector[SensorReading]): Unit = {
     // remove all state; forward switch will be false by default
     forwardingEnabled.clear()
     disableTimer.clear()
  }
}
```

