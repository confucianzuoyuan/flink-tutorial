### 使用连接的广播状态

一个常见的需求就是流应用需要将同样的事件分发到操作符的所有的并行实例中，而这样的分发操作还得是可恢复的。

我们举个例子：一条流是一个规则(比如5秒钟内连续两个超过阈值的温度)，另一条流是待匹配的流。也就是说，规则流和事件流。所以每一个操作符的并行实例都需要把规则流保存在操作符状态中。也就是说，规则流需要被广播到所有的并行实例中去。

在Flink中，这样的状态叫做广播状态(broadcast state)。广播状态和DataStream或者KeyedStream都可以做连接操作。

下面的例子实现了一个温度报警应用，应用有可以动态设定的阈值，动态设定通过广播流来实现。

```scala
val sensorData: DataStream[SensorReading] = ...
val thresholds: DataStream[ThresholdUpdate] = ...
val keyedSensorData: KeyedStream[SensorReading, String] = sensorData
  .keyBy(_.id)

// the descriptor of the broadcast state
val broadcastStateDescriptor =
  new MapStateDescriptor[String, Double](
    "thresholds", classOf[String], classOf[Double])

val broadcastThresholds: BroadcastStream[ThresholdUpdate] = thresholds
  .broadcast(broadcastStateDescriptor)

// connect keyed sensor stream and broadcasted rules stream
val alerts: DataStream[(String, Double, Double)] = keyedSensorData
  .connect(broadcastThresholds)
  .process(new UpdatableTemperatureAlertFunction())
```

带有广播状态的函数在应用到两条流上时分三个步骤：

* 调用DataStream.broadcast()来创建BroadcastStream，定义一个或者多个MapStateDescriptor对象。
* 将BroadcastStream和DataStream/KeyedStream做connect操作。
* 在connected streams上调用KeyedBroadcastProcessFunction/BroadcastProcessFunction。

下面的例子实现了动态设定温度阈值的功能。

```java
class UpdatableTemperatureAlertFunction()
    extends KeyedBroadcastProcessFunction[String,
      SensorReading, ThresholdUpdate, (String, Double, Double)] {

  // the descriptor of the broadcast state
  private lazy val thresholdStateDescriptor =
    new MapStateDescriptor[String, Double](
      "thresholds", classOf[String], classOf[Double])

  // the keyed state handle
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // create keyed state descriptor
    val lastTempDescriptor = new ValueStateDescriptor[Double](
      "lastTemp", classOf[Double])
    // obtain the keyed state handle
    lastTempState = getRuntimeContext
      .getState[Double](lastTempDescriptor)
  }

  override def processBroadcastElement(
      update: ThresholdUpdate,
      ctx: KeyedBroadcastProcessFunction[String,
        SensorReading, ThresholdUpdate,
        (String, Double, Double)]#Context,
      out: Collector[(String, Double, Double)]): Unit = {
    // get broadcasted state handle
    val thresholds = ctx
      .getBroadcastState(thresholdStateDescriptor)

    if (update.threshold != 0.0d) {
      // configure a new threshold for the sensor
      thresholds.put(update.id, update.threshold)
    } else {
      // remove threshold for the sensor
      thresholds.remove(update.id)
    }
  }

  override def processElement(
      reading: SensorReading,
      readOnlyCtx: KeyedBroadcastProcessFunction
        [String, SensorReading, ThresholdUpdate,
        (String, Double, Double)]#ReadOnlyContext,
      out: Collector[(String, Double, Double)]): Unit = {
    // get read-only broadcast state
    val thresholds = readOnlyCtx
      .getBroadcastState(thresholdStateDescriptor)
    // check if we have a threshold
    if (thresholds.contains(reading.id)) {
      // get threshold for sensor
      val sensorThreshold: Double = thresholds.get(reading.id)

      // fetch the last temperature from state
      val lastTemp = lastTempState.value()
      // check if we need to emit an alert
      val tempDiff = (reading.temperature - lastTemp).abs
      if (tempDiff > sensorThreshold) {
        // temperature increased by more than the threshold
        out.collect((reading.id, reading.temperature, tempDiff))
      }
    }

    // update lastTemp state
    this.lastTempState.update(reading.temperature)
  }
}
```

