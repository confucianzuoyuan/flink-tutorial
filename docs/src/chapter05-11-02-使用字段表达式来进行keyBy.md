### 使用字段表达式来进行keyBy

对于样例类：

```java
DataStream<SensorReading> sensorStream = ...
sensorStream.keyBy("id");
```

对于元组：

```java
DataStream<Tuple3<Integer, String, Long>> javaInput = ...
javaInput.keyBy("f2") // key Java tuple by 3rd field
```
