## 转换算子的使用

一旦我们有一条DataStream，我们就可以在这条数据流上面使用转换算子了。转换算子有很多种。一些转换算子可以产生一条新的DataStream，当然这个DataStream的类型可能是新类型。还有一些转换算子不会改变原有DataStream的数据，但会将数据流分区或者分组。业务逻辑就是由转换算子串起来组合而成的。

在我们的例子中，我们首先使用`map()`转换算子将传感器的温度值转换成了摄氏温度单位。然后，我们使用`keyBy()`转换算子将传感器读数流按照传感器ID进行分区。接下来，我们定义了一个`timeWindow()`转换算子，这个算子将每个传感器ID所对应的分区的传感器读数分配到了5秒钟的滚动窗口中。

**scala version**

```scala
val avgTemp = sensorData
  .map(r => {
    val celsius = (r.temperature - 32) * (5.0 / 9.0)
    SensorReading(r.id, r.timestamp, celsius)
  })
  .keyBy(_.id)
  .timeWindow(Time.seconds(5))
  .apply(new TemperatureAverager)
```

**java version**

```java
DataStream<T> avgTemp = sensorData
  .map(r -> {
    Double celsius = (r.temperature -32) * (5.0 / 9.0);
    return SensorReading(r.id, r.timestamp, celsius);
  })
  .keyBy(r -> r.id)
  .timeWindow(Time.seconds(5))
  .apply(new TemperatureAverager());
```

窗口转换算子将在“窗口操作符”一章中讲解。最后，我们使用了一个UDF函数来计算每个窗口的温度的平均值。我们稍后将会讨论UDF函数的实现。

