### Key选择器

方法类型

```
KeySelector[IN, KEY]
  > getKey(IN): KEY
```

两个例子

**scala version**

```scala
val sensorData = ...
val byId = sensorData.keyBy(r => r.id)
```

```scala
val input = ...
input.keyBy(value => math.max(value._1, value._2))
```

**java version**

```java
DataStream<SensorReading> sensorData = ...
KeyedStream<SensorReading, String> byId = sensorData.keyBy(r -> r.id);
```

```java
DataStream<Tuple2<Int, Int>> input = ...
input.keyBy(value -> Math.max(value.f0, value.f1));
```

