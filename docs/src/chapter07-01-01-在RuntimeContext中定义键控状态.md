### 在RuntimeContext中定义键控状态

用户自定义函数可以使用keyed state来存储和访问key对应的状态。对于每一个key，Flink将会维护一个状态实例。一个操作符的状态实例将会被分发到操作符的所有并行任务中去。这表明函数的每一个并行任务只为所有key的某一部分key保存key对应的状态实例。所以keyed state和分布式key-value map数据结构非常类似。

keyed state仅可用于KeyedStream。Flink支持以下数据类型的状态变量：

* ValueState[T]保存单个的值，值的类型为T。
  * get操作: ValueState.value()
  * set操作: ValueState.update(value: T)
* ListState[T]保存一个列表，列表里的元素的数据类型为T。基本操作如下：
  * ListState.add(value: T)
  * ListState.addAll(values: java.util.List[T])
  * ListState.get()返回Iterable[T]
  * ListState.update(values: java.util.List[T])
* MapState[K, V]保存Key-Value对。
  * MapState.get(key: K)
  * MapState.put(key: K, value: V)
  * MapState.contains(key: K)
  * MapState.remove(key: K)
* ReducingState[T]
* AggregatingState[I, O]

State.clear()是清空操作。

**scala version**

```scala
val sensorData: DataStream[SensorReading] = ...
val keyedData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)

val alerts: DataStream[(String, Double, Double)] = keyedData
  .flatMap(new TemperatureAlertFunction(1.7))

class TemperatureAlertFunction(val threshold: Double)
  extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    val lastTempDescriptor = new ValueStateDescriptor[Double](
      "lastTemp", classOf[Double])

    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
  }

  override def flatMap(
    reading: SensorReading,
    out: Collector[(String, Double, Double)]
  ): Unit = {
    val lastTemp = lastTempState.value()
    val tempDiff = (reading.temperature - lastTemp).abs
    if (tempDiff > threshold) {
      out.collect((reading.id, reading.temperature, tempDiff))
    }
    this.lastTempState.update(reading.temperature)
  }
}
```

上面例子中的FlatMapFunction只能访问当前处理的元素所包含的key所对应的状态变量。

>不同key对应的keyed state是相互隔离的。

* 通过RuntimeContext注册StateDescriptor。StateDescriptor以状态state的名字和存储的数据类型为参数。数据类型必须指定，因为Flink需要选择合适的序列化器。
* 在open()方法中创建state变量。注意复习之前的RichFunction相关知识。

当一个函数注册了StateDescriptor描述符，Flink会检查状态后端是否已经存在这个状态。这种情况通常出现在应用挂掉要从检查点或者保存点恢复的时候。在这两种情况下，Flink会将注册的状态连接到已经存在的状态。如果不存在状态，则初始化一个空的状态。

使用FlatMap with keyed ValueState的快捷方式flatMapWithState也可以实现以上需求。

**scala version**

```scala
val alerts: DataStream[(String, Double, Double)] = keyedSensorData
  .flatMapWithState[(String, Double, Double), Double] {
    case (in: SensorReading, None) =>
      // no previous temperature defined.
      // Just update the last temperature
      (List.empty, Some(in.temperature))
    case (SensorReading r, lastTemp: Some[Double]) =>
      // compare temperature difference with threshold
      val tempDiff = (r.temperature - lastTemp.get).abs
      if (tempDiff > 1.7) {
        // threshold exceeded.
        // Emit an alert and update the last temperature
        (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
      } else {
        // threshold not exceeded. Just update the last temperature
        (List.empty, Some(r.temperature))
      }
  }
```

