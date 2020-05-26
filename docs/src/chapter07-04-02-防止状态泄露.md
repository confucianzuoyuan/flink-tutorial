### 防止状态泄露

流应用通常需要运行几个月或者几年。如果state数据不断增长的话，会爆炸。所以控制state数据的大小十分重要。而Flink并不会清理state和gc。所以所有的stateful operator都需要控制他们各自的状态数据大小，保证不爆炸。

例如我们之前讲过增量聚合函数ReduceFunction/AggregateFunction，就可以提前聚合而不给state太多压力。

我们来看一个例子，我们实现了一个KeyedProcessFunction，用来计算连续两次的温度的差值，如果差值超过阈值，报警。

我们之前实现过这个需求，但没有清理掉状态数据。比如一小时内不再产生温度数据的传感器对应的状态数据就可以清理掉了。

```scala
class SelfCleaningTemperatureAlertFunction(val threshold: Double)
    extends KeyedProcessFunction[String,
      SensorReading, (String, Double, Double)] {

  // the keyed state handle for the last temperature
  private var lastTempState: ValueState[Double] = _
  // the keyed state handle for the last registered timer
  private var lastTimerState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    // register state for last temperature
    val lastTempDesc = new ValueStateDescriptor[Double](
      "lastTemp", classOf[Double])
    lastTempState = getRuntimeContext
      .getState[Double](lastTempDescriptor)
    // register state for last timer
    val lastTimerDesc = new ValueStateDescriptor[Long](
      "lastTimer", classOf[Long])
    lastTimerState = getRuntimeContext
      .getState(timestampDescriptor)
  }

  override def processElement(
      reading: SensorReading,
      ctx: KeyedProcessFunction
        [String, SensorReading, (String, Double, Double)]#Context,
      out: Collector[(String, Double, Double)]): Unit = {

    // compute timestamp of new clean up timer
    // as record timestamp + one hour
    val newTimer = ctx.timestamp() + (3600 * 1000)
    // get timestamp of current timer
    val curTimer = lastTimerState.value()
    // delete previous timer and register new timer
    ctx.timerService().deleteEventTimeTimer(curTimer)
    ctx.timerService().registerEventTimeTimer(newTimer)
    // update timer timestamp state
    lastTimerState.update(newTimer)

    // fetch the last temperature from state
    val lastTemp = lastTempState.value()
    // check if we need to emit an alert
    val tempDiff = (reading.temperature - lastTemp).abs
    if (tempDiff > threshold) {
      // temperature increased by more than the threshold
      out.collect((reading.id, reading.temperature, tempDiff))
    }

    // update lastTemp state
    this.lastTempState.update(reading.temperature)
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String,
        SensorReading, (String, Double, Double)]#OnTimerContext,
      out: Collector[(String, Double, Double)]): Unit = {

    // clear all state for the key
    lastTempState.clear()
    lastTimerState.clear()
  }
}
```

