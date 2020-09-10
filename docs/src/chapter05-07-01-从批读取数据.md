### 从批读取数据

**scala version**

```scala
val stream = env
  .fromElements(
    SensorReading("sensor_1", 1547718199, 35.80018327300259),
    SensorReading("sensor_6", 1547718199, 15.402984393403084),
    SensorReading("sensor_7", 1547718199, 6.720945201171228),
    SensorReading("sensor_10", 1547718199, 38.101067604893444)
  )
```

**java version**

```java
DataStream<SensorReading> stream = env
  .fromElements(
    new SensorReading("sensor_1", 1547718199, 35.80018327300259),
    new SensorReading("sensor_6", 1547718199, 15.402984393403084),
    new SensorReading("sensor_7", 1547718199, 6.720945201171228),
    new SensorReading("sensor_10", 1547718199, 38.101067604893444)
  )
```