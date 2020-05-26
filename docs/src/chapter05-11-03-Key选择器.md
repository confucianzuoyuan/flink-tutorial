### Key选择器

方法类型

```
KeySelector[IN, KEY]
  > getKey(IN): KEY
```

两个例子

```scala
val sensorData: DataStream[SensorReading] = ...
val byId: KeyedStream[SensorReading, String] = sensorData.keyBy(r => r.id)
```

```scala
val input: DataStream[(Int, Int)] = ...
val keyedStream = input.keyBy(value => math.max(value._1, value._2))
```

