### Key选择器

方法类型

```
KeySelector[IN, KEY]
  > getKey(IN): KEY
```

两个例子

```java
DataStream[SensorReading] sensorData = ...
KeyedStream<SensorReading, String> byId = sensorData.keyBy(r -> r.id);
```

```java
DataStream<(Integer, Integer)> input = ...
input.keyBy(value -> Math.max(value.f0, value.f1));
```

